package spinoco.fs2.kafka

import java.net.UnknownHostException
import java.util.Date

import cats.effect.{IO, Ref}
import cats.effect.unsafe.implicits.global
import fs2._
import shapeless.tag.@@
import spinoco.fs2.kafka.failure.{BrokerReportedFailure, LeaderNotAvailable}
import spinoco.fs2.kafka.network.BrokerAddress
import spinoco.protocol.kafka.Request.OffsetsRequest
import spinoco.protocol.kafka.Response.{OffsetResponse, PartitionOffsetResponse}
import spinoco.protocol.kafka._

import scala.concurrent.duration._

/**
  * Offset range query against a fake DNS. Host missing in DNS simulates pod that is restarting.
  */
class QueryOffsetRangeSpec extends Fs2KafkaClientSpec {

  implicit val logger: Logger[IO] = new Logger[IO] {
    def log(level: Logger.Level.Value, msg: => String, throwable: Throwable): IO[Unit] = IO.unit
  }

  val testTopic = topic("test-topic")
  val part0 = partition(0)

  val seedA = BrokerAddress("broker-a", 9092)
  val seedB = BrokerAddress("broker-b", 9092)

  def resolve(dns: Ref[IO, Set[String]], address: BrokerAddress): IO[Unit] =
    dns.get flatMap { hosts =>
      if (hosts.contains(address.host)) IO.unit
      else IO.raiseError(new UnknownHostException(address.host))
    }

  def mkDns(hosts: String*): IO[Ref[IO, Set[String]]] = Ref.of[IO, Set[String]](hosts.toSet)

  def run[A](io: IO[A]): A = io.unsafeRunTimed(30.seconds).getOrElse(fail("Timed out"))

  // head offset tells which broker served the request
  def offsetsFrom(leader: BrokerAddress): OffsetResponse = {
    val head = if (leader == seedA) 1L else 2L
    OffsetResponse(Vector((testTopic, Vector(PartitionOffsetResponse(part0, None, new Date(0), Vector(offset(head), offset(10)))))))
  }

  def offsetConnection(dns: Ref[IO, Set[String]])(address: BrokerAddress): Pipe[IO, OffsetsRequest, OffsetResponse] =
    _.evalMap { _ => resolve(dns, address) as offsetsFrom(address) }

  def queryOffsetRange(
    getLeader: (String @@ TopicName, Int @@ PartitionId) => IO[Option[BrokerAddress]]
    , connection: BrokerAddress => Pipe[IO, OffsetsRequest, OffsetResponse]
  ): IO[(Long, Long)] = {
    KafkaClient.impl.queryOffsetRange[IO](
      getLeader = getLeader
      , brokerOffsetConnection = connection
      , maxTimeForQuery = 1.second
      , leaderFailureTimeout = 10.millis
      , leaderFailureMaxAttempts = 20
    )(testTopic, part0)
    .map { case (head, tail) => (head: Long, tail: Long) }
  }


  "queryOffsetRange" - {

    "retries until leader DNS record is back" in {
      val result = {
        run(mkDns() flatMap { dns =>
          val restorePod = IO.sleep(100.millis) >> dns.update(_ + "broker-a")
          restorePod.start >> queryOffsetRange((_, _) => IO.pure(Some(seedA)), offsetConnection(dns))
        })
      }
      result shouldBe ((1L, 10L))
    }

    "follows leader change after failure" in {
      val result = {
        run(mkDns("broker-b") flatMap { dns =>
          Ref.of[IO, Int](0) flatMap { lookups =>
            def getLeader(topicId: String @@ TopicName, partition: Int @@ PartitionId): IO[Option[BrokerAddress]] = {
              lookups.getAndUpdate(_ + 1) map { n => if (n == 0) Some(seedA) else Some(seedB) }
            }
            queryOffsetRange(getLeader, offsetConnection(dns))
          }
        })
      }
      result shouldBe ((2L, 10L))
    }

    "fails with last failure when leader does not come back" in {
      val result = run(mkDns() flatMap { dns => queryOffsetRange((_, _) => IO.pure(Some(seedA)), offsetConnection(dns)).attempt })
      result.left.toOption.map(_.getClass) shouldBe Some(classOf[UnknownHostException])
    }

    "does not retry error reported by broker" in {
      val failure = OffsetResponse(Vector((testTopic, Vector(PartitionOffsetResponse(part0, Some(ErrorType.UNKNOWN), new Date(0), Vector.empty)))))
      val result = {
        run(queryOffsetRange((_, _) => IO.pure(Some(seedA)), _ => _.map(_ => failure)).attempt)
      }
      result.left.toOption.collect { case BrokerReportedFailure(_, _, err) => err } shouldBe Some(ErrorType.UNKNOWN)
    }

    "fails when no leader is known" in {
      run(queryOffsetRange((_, _) => IO.pure(None), _ => _ => Stream.empty).attempt) shouldBe Left(LeaderNotAvailable(testTopic, part0))
    }

  }

}
