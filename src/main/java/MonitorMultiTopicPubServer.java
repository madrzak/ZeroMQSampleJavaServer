import org.zeromq.ZMQ;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Lukasz Madrzak on 21/03/17.
 */
public class MonitorMultiTopicPubServer {
    private static final String TOPIC_M = "M";
    private static final String TOPIC_R = "R";
    private static final String TOPIC_A = "A";
    private static final String[] topics = new String[] {TOPIC_M, TOPIC_R, TOPIC_A};


    public static void main(String[] args) throws Exception {

        new Thread(() -> {
            //  Prepare our context and publisher
            ZMQ.Context context = ZMQ.context(1);

            ZMQ.Socket publisher = context.socket(ZMQ.PUB);
            publisher.bind("tcp://*:5556");
            publisher.bind("ipc://monitors");

            Random srandom = new Random(System.currentTimeMillis());
            while (!Thread.currentThread().isInterrupted()) {
                //  Get values that will fool the boss
                long time, monitorId;
                monitorId = 5000 + srandom.nextInt(50);
                time = System.currentTimeMillis();

                //  Send message to all subscribers
                String update = String.format("tenant1 %d %d", monitorId, time);
                String topic = topics[ThreadLocalRandom.current().nextInt(0, 2 + 1)];
                publisher.sendMore(topic);
                publisher.send(update, 0);
                System.out.println(topic + " " + update);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    System.out.println("interrupted " + e.toString());
                }
            }
            publisher.close();
            context.term();
        }).start();

        new Thread(() -> {
            //  Prepare our context and publisher
            ZMQ.Context context = ZMQ.context(1);

            ZMQ.Socket publisher = context.socket(ZMQ.PUB);
            publisher.bind("tcp://*:5557");
            publisher.bind("ipc://monitors2");

            Random srandom = new Random(System.currentTimeMillis());
            while (!Thread.currentThread().isInterrupted()) {
                //  Get values that will fool the boss
                long time, monitorId;
                monitorId = 5000 + srandom.nextInt(50);
                time = System.currentTimeMillis();

                //  Send message to all subscribers
                String update = String.format("tenant2 %d %d", monitorId, time);
                String topic = topics[ThreadLocalRandom.current().nextInt(0, 2 + 1)];
                publisher.sendMore(topic);
                publisher.send(update, 0);
                System.out.println(topic + " " + update);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    System.out.println("interrupted " + e.toString());
                }
            }
            publisher.close();
            context.term();
        }).start();

    }
}