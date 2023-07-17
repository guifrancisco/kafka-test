package com.gui.strproducer.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;


@Service
@RequiredArgsConstructor
@Log4j2
public class StringProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message){

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("str-topic", message);

        future.thenAccept(result -> {
                    if(result != null){
                        log.info("Send message with success {}", message);
                        log.info("Partition {}, Offset {}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                })
                .exceptionally(ex -> {
                    log.info("Error send message");
                    return null;
                });
    }
}
