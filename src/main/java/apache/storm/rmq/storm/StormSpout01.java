package apache.storm.rmq.storm;

//package apache.storm.rmq.storm.StormSpout01;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.TopologyBuilder;

import org.apache.storm.LocalCluster;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;

import io.latent.storm.rabbitmq.config.ConnectionConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfig;
import io.latent.storm.rabbitmq.config.ConsumerConfigBuilder;

import org.apache.storm.topology.IRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;

import java.util.Map;

public class StormSpout01 extends BaseRichSpout {
//  Scheme scheme;

    SpoutOutputCollector _collector;
    public final static String QUEUE_NAME = "vitalsigns";
    ConnectionFactory factory;
    Connection connection;
    Channel channel;
    QueueingConsumer consumer;
    //  Scheme scheme ;
    //  IRichSpout spout ;

    @Override
    /* Open method of the spout , here we initialize the prefetch count ,  
this parameter specified how many messages would be prefetched
from the queue by the spout – to increase the efficiency of the     solution */
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        System.out.println(" Intilization of spout..... ");

        try {
            factory = new ConnectionFactory();
            System.out.println(" creating connection factory..... ");

            factory.setHost("localhost");

            System.out.println(" setting host to localhost/127.0.0.1 .... ");
            connection = factory.newConnection();
            System.out.println(" creating new connection..... ");
            channel = connection.createChannel();
            System.out.println(" creating new channel..... ");
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            System.out.println(" Declaring queue..... ");
            System.out.println(" Waiting for messages... . ");
        } catch (Exception exception) {
            System.out.println("Exception occurred. " + exception.getMessage());
        }

    }

    @Override

///  the Method witch pool Events from Data source 
    public void nextTuple() {
        System.out.println("In wait of tuples.... ");
        try {
            consumer = new QueueingConsumer(channel);
            System.out.println(" trying to consume..... ");
            channel.basicConsume(QUEUE_NAME, true, consumer);

            while (true) {
                System.out.println(" trying to deliver..... ");
                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                String message = new String(delivery.getBody());
                System.out.println(" getting string..... ");
                System.out.println(" Vital Signs Received '" + message + "'");
                System.out.print("emitting Rabbitmq Queue tuple");
                _collector.emit(new Values(message));
                System.out.print("emitted Rabbitmq Queue tuple");
            }
        } /* catch(IOException io)
{
System.out.println("Exception occurred. ");
}*/ catch (Exception exception) {
            System.out.println("Exception occurred. ");
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("vital sign"));
    }

    public static void app1(String[] args) throws Exception {

        IRichSpout RabbitMQspout = new StormSpout01();

        ConnectionConfig connectionConfig = new ConnectionConfig("localhost", 5672, "guest", "guest", ConnectionFactory.DEFAULT_VHOST, 10);
        // host, port, username, password, virtualHost, heartBeat 
        ConsumerConfig spoutConfig = new ConsumerConfigBuilder().connection(connectionConfig)
                .queue("vitalsigns")
                .prefetch(200)
                .requeueOnFail()
                .build();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("my-spout", RabbitMQspout)
                .addConfigurations(spoutConfig.asMap())
                .setMaxSpoutPending(200);

        Config conf = new Config();
        //Set to false to disable debug information when
        // running in production on a cluster
        conf.setDebug(false);

        //If there are arguments, we are running on a cluster
        if (args != null && args.length > 0) {
            //parallelism hint to set the number of workers
            conf.setNumWorkers(3);
            //submit the topology
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } //Otherwise, we are running locally
        else {
            //Cap the maximum number of executors that can be spawned
            //for a component to 3
            conf.setMaxTaskParallelism(3);
            //LocalCluster is used to run locally
            LocalCluster cluster = new LocalCluster();
            //submit the topology
            cluster.submitTopology("RabbitMQ-spout", conf, builder.createTopology());
            //sleep
            Thread.sleep(10000);
            //shut down the cluster
            cluster.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Hello storm");
        app1(args);
    }
;

}/*  from:
https://github.com/ppat/storm-rabbitmq
Storm RabbitMQ is a library of tools to be employed while integrating with RabbitMQ from Storm ....
http://grokbase.com/t/gg/storm-user/1342d7qqp3/rabbitmq-blocking-at-new-connection-creation-within-storm-topology
*/
