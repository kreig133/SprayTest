import scala.concurrent.Future
import scala.xml.XML

import akka.actor._
import akka.pattern.{ask, pipe}
import akka.routing._

import spray.client.pipelining._
import spray.json.DefaultJsonProtocol._
import spray.http._
import spray.httpx.SprayJsonSupport._
import spray.routing.SimpleRoutingApp
import spray.util._
import StatusCodes._

object Main extends App with SimpleRoutingApp  {

  implicit val system  = ActorSystem("my-system")
  implicit val timeout = akka.util.Timeout(5000)

  import system.dispatcher

  val supervisor = system.actorOf(Props[Supervisor])

  startServer(interface = "localhost", port = 8080) {
    path("search") {
      (get & parameterSeq) { params: Seq[(String, String)] =>
        params.groupBy(_._1).map{e => (e._1, e._2.map(_._2))}.get("query") match {
          case Some(seq) =>
            complete {
              (supervisor ? seq).mapTo[Either[String, Map[String, Int]]].map {
                case Right(map) => map
                case Left (msg) => Map(msg -> 0, "errorCode" -> 502)
              }
            }
          case _ => complete(BadRequest, "No queries for search!!!")
        }
      }
    } ~ complete(NotFound)
  }
}

class Supervisor extends Actor with ActorLogging {
  import context.system
  import context.dispatcher

  val router: ActorRef =
    context.actorOf(Props(classOf[Worker]).withRouter(RoundRobinRouter(nrOfInstances = 10)))

  implicit val timeout = akka.util.Timeout(10000)

  val Extract = """^https?://(?:[^.]*\.)*?(\w+\.\w+)(?:/.*)?$""".r

  override def receive: Receive = {
    case x: Seq[String] =>
      log.debug("Received query for search : " + x)
      Future.traverse(x) {
        query => (router ? query).mapTo[Seq[String]]
      }.map {
        responces =>

          val links: Seq[String] = responces.flatten.distinct

          Right(links.groupBy {
            case Extract(secondLevel) => secondLevel
          }.map(e => (e._1, e._2.size)))

      }.mapTo[Either[String, Map[String, Int]]].pipeTo(sender).onFailure {
        case e: Throwable => Left( s"""ErrorClass: ${e.getClass()}, ErrorMessage: ${e.getMessage()}""")
      }
  }
}

class Worker extends Actor with Stash with ActorLogging {

  import context.dispatcher

  implicit val timeout = akka.util.Timeout(10000)

  val pipeline: HttpRequest => Future[HttpResponse] = sendReceive

  override def receive: Receive = readyForJob

  def readyForJob: Receive = {
    case query: String =>
      log.debug("Starting new request for search...")
      pipeline(Get(s"http://blogs.yandex.ru/search.rss?text=${java.net.URLEncoder.encode(query)}")).map {
        case HttpResponse(OK, entity, _, _) =>

          self ! Done

          val root = XML.loadString(entity.asString)

          // parse feed links
          (root \\ "item" \\ "link").map(_.text).toList

      }.pipeTo(sender)

      context.become(busy)
      log.debug("...now is busy.")
  }

  def busy: Receive = {
    case Done =>
      log.debug("Work finished. Unstashing message.")
      unstashAll()
      context.become(readyForJob)
      log.debug("Now ready for work!")
    case _ => stash()
  }

  case object Done
}