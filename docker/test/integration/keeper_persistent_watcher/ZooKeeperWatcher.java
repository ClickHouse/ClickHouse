import org.apache.zookeeper.*;
import org.w3c.dom.events.Event;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperWatcher implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:9181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String NODE_PATH = "/testPersistentWatch2";
    private static final String FAKE_PATH = "/fakeNode";

    private ZooKeeper zooKeeper;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    public static void main(String[] args) throws Exception {
        ZooKeeperWatcher test = new ZooKeeperWatcher();
        test.connect();
        System.out.println("Connected!");

        test.testPersistentWatch();
    }

    public void connect() throws IOException, InterruptedException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        connectedSignal.await();
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
        }
        if (event.getType() == Event.EventType.NodeDataChanged) {
            System.out.println("PERSISTENT Watch triggered: " + event.getPath());
        }
    }

    public void testPersistentWatch() throws Exception {
        if (zooKeeper.exists(NODE_PATH, false) == null) {
            zooKeeper.create(NODE_PATH, "Initial data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        if (zooKeeper.exists(FAKE_PATH, false) == null) {
            zooKeeper.create(FAKE_PATH, "Initial data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        try {
            System.out.println("Trying to add PERSISTENT watch...");
            zooKeeper.addWatch(NODE_PATH, this, AddWatchMode.PERSISTENT);
            System.out.println("PERSISTENT watch added successfully!");
        } catch (Exception e) {
            System.out.println("Failed to add PERSISTENT watch: " + e.getMessage());
        }

        Thread.sleep(300);
        System.out.println("Updating node data...");
        zooKeeper.setData(NODE_PATH, "New data".getBytes(), -1);

        Thread.sleep(300);
        System.out.println("Updating node data again...");
        zooKeeper.setData(NODE_PATH, "New data 2".getBytes(), -1);

        Thread.sleep(300);
        System.out.println("Updating fake node data...");
        zooKeeper.setData(FAKE_PATH, "New data 2".getBytes(), -1);
        try {
            System.out.println("Trying to remove PERSISTENT watch...");
            zooKeeper.removeWatches(NODE_PATH, this, WatcherType.Any, true);
            System.out.println("PERSISTENT watch removed successfully!");
        } catch (Exception e) {
            System.out.println("Failed to remove PERSISTENT watch: " + e.getMessage());
        }

        Thread.sleep(300);
        System.out.println("Updating node data again...");
        zooKeeper.setData(NODE_PATH, "Another update".getBytes(), -1);

        System.out.println("Test finished.");
    }
}
