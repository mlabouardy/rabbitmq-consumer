package com.labouardy.app;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;


public class RabbitMQConsumer {
	
	
	public static Connection getConnection() throws IOException, TimeoutException{
		ConnectionFactory factory=new ConnectionFactory();
		factory.setUsername("guest");
		factory.setPassword("guest");
		factory.setHost("127.0.0.1");
		factory.setPort(32771);
		return factory.newConnection();
	}
	
	
	public static void main(String[] args) throws IOException, TimeoutException {
		Connection conn=getConnection();
		
		Channel channel = conn.createChannel();
	    String exchangeName = "vrp";
	    String queueName = "vrp";
	    String routingKey = "test";
	    
	    boolean durable = true;
	    channel.exchangeDeclare(exchangeName, "direct", durable);
	    channel.queueDeclare(queueName, durable,false,false,null);
	    channel.queueBind(queueName, exchangeName, routingKey);
	    
	    boolean noAck = false;
	    QueueingConsumer consumer = new QueueingConsumer(channel);
	    channel.basicConsume(queueName, noAck, consumer);
	    boolean runInfinite = true;
	    while (runInfinite) {
	      QueueingConsumer.Delivery delivery;
	      try {
	          delivery = consumer.nextDelivery();
	      } catch (InterruptedException ie) {
	         continue;
	      }
	      System.out.println(new String(delivery.getBody()));
	      channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
	  }
	  channel.close();
	  conn.close();
	}
}
