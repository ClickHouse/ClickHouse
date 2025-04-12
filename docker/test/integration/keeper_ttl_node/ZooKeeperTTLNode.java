import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperTTLNode {
    private static final String ZK_ADDRESS = "localhost:9181";
    private static final int SESSION_TIMEOUT = 5000;
    private static final String NODE_PATH = "/my_ttl_node";
    private static final long TTL = 3000;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        CountDownLatch connectedSignal = new CountDownLatch(1);

        ZooKeeper zk = new ZooKeeper(ZK_ADDRESS, SESSION_TIMEOUT, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });

        connectedSignal.await();
        System.out.println("Connected!");

        Stat stat = zk.exists(NODE_PATH, false);
        System.out.println("Existance before creating: " + (stat != null));

        if (zk.exists(NODE_PATH, false) != null) {
            zk.delete(NODE_PATH, -1);
        }

        Thread.sleep(2000);
        zk.create(NODE_PATH, "temp data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_WITH_TTL, new Stat(), TTL);
        System.out.println("TTL node created: " + NODE_PATH);

        stat = zk.exists(NODE_PATH, false);
        System.out.println("Node exists immediately after create: " + (stat != null));

        System.out.println("Waiting for TTL to expire...");
        Thread.sleep(TTL + 2000);

        stat = zk.exists(NODE_PATH, false);
        System.out.println("Node exists after TTL: " + (stat != null));

        zk.close();
    }
}
