package sample

import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.{GroupAuthorizationException, SaslAuthenticationException, TopicAuthorizationException, WakeupException}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.{Collections, Properties}
import java.util.concurrent.CountDownLatch
import scala.sys.exit

/**
 * Scala 기반으로 구성된 Kafka Consumer Client 샘플 프로그램입니다.
 *
 *
 * kafka 서버 정보, 토픽명, 유저명과 비밀번호 등을 직접 코드에 작성하여 사용하거나
 * 실행 시 인자로 주어 테스트를 진행할 수 있습니다.
 *
 * 접근제어에 관련된 내용은 init() 메소드에 주석으로 설명되어 있습니다.
 *
 * Kafka Consumer Client 구성에 대한 자세한 설명은
 * https://www.conduktor.io/kafka/complete-kafka-consumer-with-java 에서 확인 가능합니다.
 */
object Consumer {
  private final val log = LoggerFactory.getLogger(this.getClass)

  private var server = "localhost:9092" // kafka broker 정보 IP:PORT
  private var topic = "default-topic"   // 구독할 토픽명
  private var group = "default-group"   // consumer group명

  private var username = "kafka"        // kafka 접속 유저명
  private var password = "kafka"        // kafka 접속 비밀번호

  private var consumer: KafkaConsumer[String, String] = _

  private var flag = true
  private val latch = new CountDownLatch(1)

  def main(args: Array[String]): Unit = {

    // arguments 처리
    if (args.length % 2 != 0) {
      println(usage)
      exit(1)
    }

    args.sliding(2, 2).toList.collect {
      case Array("--server", server: String) => this.server = server
      case Array("--topic", topic: String) =>   this.topic = topic
      case Array("--group", group: String) =>   this.group = group
      case Array("--u", username: String) =>    this.username = username
      case Array("--pw", password: String) =>   this.password = password
      case _ =>                                 println(usage); exit(1)
    }

    // 종료 프로세스
    val mainThread = Thread.currentThread()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        consumer.wakeup()
        try {
          mainThread.join()
        } catch {
          case e: InterruptedException => e.printStackTrace()
        }
      }
    })

    // Consumer 프로세스
    init()
    consume()

    latch.await()

    // 종료
    consumer.close()
    log.info("Consumer closed.")
  }

  private def init(): Unit = {
    val props = new Properties()

    // 기본 properties
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // 접근제어 properties
    // SECURITY_PROTOCOL_CONFIG : 보안 프로토콜을 설정합니다. 현재 서버에 적용된 값은 SASL_PLAINTEXT입니다.
    // SASL_MECHANISM : SASL 메커니즘을 설정합니다. 현재 서버에 적용된 값은 "PLAIN"입니다.
    // SASL_JAAS_CONFIG : SASL 구성 정보를 작성합니다.
    //   - org.apache.kafka.common.security.plain.PlainLoginModule required : 서버에 적용된 로그인 모듈인 PlainLoginModule로 설정합니다.
    //   - username, password : 클라이언트가 kafka 서버에 접근하기 위한 유저명과 비밀번호 정보를 설정합니다.
    //                          서버에 해당 유저명과 비밀번호에 맞는 값이 없거나, 토픽에 대해 해당 유저가 가진 권한이 없을 때는 접근 불가합니다.
    //   - 맨 마지막에 세미콜론(;)을 반드시 붙여 주어야 합니다.
    props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name)
    props.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN")
    props.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
      s"org.apache.kafka.common.security.plain.PlainLoginModule required username='$username' password='$password';"
    )

    consumer = new KafkaConsumer[String, String](props)

    try {
      consumer.subscribe(Collections.singletonList(topic))
      log.info(s"Subscribe topic : $topic")
    } catch {
      case e: Exception => log.warn(s"Consumer initialize error : ${e.getMessage}"); e.printStackTrace()
    }
  }

  private def consume(): Unit = {
    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))

        // 읽어온 메시지 처리 (key, value, partition, offset 정보를 로그로 출력합니다)
        records.forEach {
          record =>
            log.info(
              s"""Key : ${record.key()}, Value : ${record.value()}
                 |Partition : ${record.partition()}, Offset : ${record.offset()}
                 |""".stripMargin)
        }

        consumer.commitAsync()

        Thread.sleep(100)
      }
    } catch {
      case _: TopicAuthorizationException => log.warn(s"Not authorized to access topic : $topic") // Topic 접근 권한이 없을 때 발생하는 Exception
      case _: GroupAuthorizationException => log.warn(s"Not authorized to access group : $group") // Consumer Group에 대해 권한이 없을 때 발생하는 Exception
      case _: SaslAuthenticationException => log.warn("Authentication failed : Invalid username or password") // 해당 유저, 비밀번호 정보가 서버에 없을 때 발생하는 Exception
      case _: WakeupException =>
      case e: Exception => log.warn(s"Consume error : ${e.getMessage}"); e.printStackTrace()
    } finally {
      latch.countDown()
    }
  }

  val usage: String =
    s"""* ARGUMENTS USAGE
       |--server : broker server IP:PORT [default : "$server"]
       |--topic : topic name [default : "$topic"]
       |--group : consumer group name [default : "$group"]
       |--u : username [default : "$username"]
       |--pw : password [default : "$password"]
       |""".stripMargin
}
