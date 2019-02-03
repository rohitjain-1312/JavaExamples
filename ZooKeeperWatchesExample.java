import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

public class ZooKeeperWatchesExample {

    private static String NODE_PATH1 = "/zoo/node1";
    private static String NODE_PATH2 = "/zoo/node2";

    public static void main(String[] args) throws Exception {
        String zkHosts = "localhost:9881";
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        CuratorFramework curator = CuratorFrameworkFactory.newClient(zkHosts, retryPolicy);
        curator.start();

        if (null == curator.checkExists().forPath(NODE_PATH1)) {
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                    .forPath(NODE_PATH1);
        }

        if (null == curator.checkExists().forPath(NODE_PATH2)) {
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                    .forPath(NODE_PATH2);
        }

        checkSignal(curator);

        while (true) {
            System.out.println("Going to set randomdata1");
            curator.setData().forPath(NODE_PATH1, "randomdata1".getBytes());
            Thread.sleep(5 * 1000);
            System.out.println("Going to set randomdata2");
            curator.setData().forPath(NODE_PATH1, "randomdata2".getBytes());
            Thread.sleep(5 * 1000);
            System.out.println("Going to set randomdata3");
            curator.setData().forPath(NODE_PATH2, "randomdata3".getBytes());
            Thread.sleep(5 * 1000);
        }
    }

    private static void checkSignal(CuratorFramework curator) {
        try {
            curator.getData().usingWatcher(new CuratorWatcher() {
                @Override
                public void process(WatchedEvent watchedEvent) throws Exception {
                    if (watchedEvent.getType().equals(Watcher.Event.EventType.NodeDataChanged)) {
                        System.out.println("Node data changed..!");
                        System.out.println("New Data: " + new String(curator.getData().forPath(NODE_PATH1)));
                    }
                    checkSignal(curator);
                }
            }).forPath(NODE_PATH1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
