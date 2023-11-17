import pytest
import time
from collections import namedtuple
from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.keeper_utils import KeeperClient
from helpers.network import PartitionManager


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/keeper_config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        node.query("DROP TABLE IF EXISTS simple SYNC")
        node.query(
            """
        CREATE TABLE simple (date Date, id UInt32)
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/0/simple', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id;
        """.format(
                replica=node.name
            )
        )
        yield cluster

    finally:
        cluster.shutdown()


ConnectionInfo = namedtuple('ConnectionInfo', ['host', 'is_local_az'])


def parse_connection_info(row):
    info = row.strip().split('\t')
    assert(len(info) == 2)
    return ConnectionInfo(info[0], info[1] == '1')


def assert_same_az_keeper(node: ClickHouseInstance, zk_node: str):
    def check_callback(host):
        info = parse_connection_info(host)
        return info.host == zk_node and info.is_local_az

    host = node.query_with_retry(
        "select host, connected_local_az_host from system.zookeeper_connection", check_callback=check_callback
    )
    assert check_callback(host)


def assert_different_az_keeper(node: ClickHouseInstance, zk_node: str):
    def check_callback(row):
        info = parse_connection_info(row)
        return info.host != zk_node and (not info.is_local_az)

    row = node.query_with_retry(
        "select host, connected_local_az_host from system.zookeeper_connection", check_callback=check_callback
    )
    assert check_callback(row)


def assert_session_id_increase(value, timeout):
    start_session_id_str = node.query_with_retry(
        "select client_id from system.zookeeper_connection"
    )
    start = int(start_session_id_str)
    def check_session_id(xid):
        return int(xid) >= value + start
    session_id = node.query_with_retry(
        "select client_id from system.zookeeper_connection",
        check_callback=check_session_id,
        timeout=timeout,
    )
    assert int(session_id) >= value + start


def test_get_availability_zone():
    with KeeperClient.from_cluster(cluster, "zoo1") as client1:
        assert client1.get("/keeper/availability_zone") == "az-zoo1"

    # Keeper2 set enable_auto_detection_on_cloud to true, but is ignored and <value>az-zoo2</value> is used.
    with KeeperClient.from_cluster(cluster, "zoo2") as client2:
        assert client2.get("/keeper/availability_zone") == "az-zoo2"
        assert "availability_zone" in client2.ls("/keeper")

    # keeper3 is not configured with availability_zone value.
    with KeeperClient.from_cluster(cluster, "zoo3") as client3:
        with pytest.raises(Exception):
            client3.get("/keeper/availability_zone")


def test_connect_local_az_keeper(started_cluster: ClickHouseCluster):
    # Initially the node must connect to its own local az keeper host.
    assert_same_az_keeper(node, "zoo2")

    with PartitionManager() as pm:
        pm._add_rule(
            {
                "source": node.ip_address,
                "destination": cluster.get_instance_ip("zoo2"),
                "action": "REJECT --reject-with tcp-reset",
            }
        )
        assert_different_az_keeper(node, "zoo2")
        node.query_with_retry("INSERT INTO simple VALUES ({0}, {0})".format(1))
        assert (
            node.query_with_retry(
                "SELECT count() from simple",
                check_callback=lambda count: count.strip() == "1",
            )
            == "1\n"
        )

    # at this point network partitioning has been reverted.
    # the nodes should switch to zoo2 automatically because of avaibility zone load balancing.
    # otherwise they would connect to a random replica.
    assert_same_az_keeper(node, "zoo2")
    node.query_with_retry("INSERT INTO simple VALUES ({0}, {0})".format(2))


def test_stay_connected_non_local_keeper_unavailable():
    assert_same_az_keeper(node, "zoo2")
    with PartitionManager() as pm:
        pm._add_rule(
            {
                "source": node.ip_address,
                "destination": cluster.get_instance_ip("zoo1"),
                "action": "REJECT --reject-with tcp-reset",
            }
        )
        # Ensure that clickhouse stay connected to zoo2. zoo1's unavailability should be irrelevant.
        # Check 5 seconds since we configure the fallback session max time 4 seconds.
        for i in range(5):
            assert_same_az_keeper(node, "zoo2")
            time.sleep(1)
        assert_same_az_keeper(node, "zoo2")


def test_retry_unknown_keeper():
    with PartitionManager() as pm:
        pm._add_rule(
            {
                "source": node.ip_address,
                "destination": cluster.get_instance_ip("zoo2"),
                "action": "REJECT --reject-with tcp-reset",
            }
        )
        # Restart the server to make it forget about the mapping from keeper host to the availabilit zone.
        node.stop_clickhouse()
        node.start_clickhouse()
        assert_different_az_keeper(node, 'zoo2')

        # Check a few times that node is disconnecting and reconnecting, because zoo2 availability zone is still unresolved yet.
        # 2 * 3 (fallback session max time) = 6 seconds, so 8 seconds should be enough for us to observe two new sessions.
        assert_session_id_increase(2, timeout=8)
        # Still not using zoo2.
        assert_different_az_keeper(node, 'zoo2')
    assert_same_az_keeper(node, "zoo2")


def test_basics_when_no_keeper_available():
    with PartitionManager() as pm:
        for keeper in ["zoo1", "zoo2", "zoo3"]:
            pm._add_rule(
                {
                    "source": node.ip_address,
                    "destination": cluster.get_instance_ip(keeper),
                    "action": "REJECT --reject-with tcp-reset",
                }
            )
        assert(node.query_with_retry("SELECT 1") == "1\n")
