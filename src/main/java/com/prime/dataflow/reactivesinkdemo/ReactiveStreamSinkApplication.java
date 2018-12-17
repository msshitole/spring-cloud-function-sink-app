package com.prime.dataflow.reactivesinkdemo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Sink;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.ErrorMessage;
import reactor.core.publisher.Flux;

import java.util.function.Consumer;

@SpringBootApplication
@EnableBinding(Sink.class)
public class ReactiveStreamSinkApplication {

    public static void main(String[] args) {
        SpringApplication.run(ReactiveStreamSinkApplication.class,
                "--spring.cloud.stream.function.definition=myConsumer");
    }

    @Bean
    public Consumer<Flux<String>> myConsumer() {

        return stream -> stream.log()
                               .subscribe(value -> {
            if ("foo".equalsIgnoreCase(value)) {
                throw new RuntimeException("BOOM!");
            }
            System.out.println("Received value: " + value);
        });

    }

    @StreamListener("errorChannel")
    public void error(ErrorMessage message) {
        // log the error
        System.out.println("Handling ERROR: " + message);
    }

    /**
     * Processing msgs from the DLQ
     * @param failedMessage
     */
    @KafkaListener(id= "errId", topics = "dlq-topic")
    public void rePublish(Message failedMessage) {
        //we can write to a database or move to a parking lot queue
        System.out.println("Writing to database: "+failedMessage.getPayload());
    }

}
