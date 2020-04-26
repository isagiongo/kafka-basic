package com.isagiongo.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static final String BOOTSTRAP_SERVER = "localhost:9092";

    public static void main(String[] args) {
        //Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Oi gentes");

        //Send data
        producer.send(record);

        //flush data
        producer.flush();

        //flush and close producer
        producer.close();

    }
}
