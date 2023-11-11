import pytest
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


def assert_uses_zk_node(node: ClickHouseInstance, zk_node):
    def check_callback(host):
        return host.strip() == zk_node

    # We don't convert the column 'host' of system.zookeeper_connection to ip address any more.
    host = node.query_with_retry(
        "select host from system.zookeeper_connection", check_callback=check_callback
    )
    assert host.strip() == zk_node


def assert_not_uses_zk_node(node: ClickHouseInstance, zk_node):
    def check_callback(host):
        return host.strip() != zk_node

    # We don't convert the column 'host' of system.zookeeper_connection to ip address any more.
    host = node.query_with_retry(
        "select host from system.zookeeper_connection", check_callback=check_callback
    )
    assert host.strip() != zk_node


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


# TODO: more test cases
# - add another node with different az first.
# - session_uptime from system.zookeeper_connection: if it's local az, no timeout, otherwise, timeout
# - session id: from non local az to local, very quick retry, should have different session id. check whether it's feasbile and reliable.
# - no host available, should still be able to serve `select 1`, no crash.
def test_connect_local_az_keeper(started_cluster: ClickHouseCluster):
    # Initially the node must connect to its own local az keeper host.
    assert_uses_zk_node(node, "zoo2")

    with PartitionManager() as pm:
        pm._add_rule(
            {
                "source": node.ip_address,
                "destination": cluster.get_instance_ip("zoo2"),
                "action": "REJECT --reject-with tcp-reset",
            }
        )
        assert_not_uses_zk_node(node, "zoo2")
        node.query_with_retry("INSERT INTO simple VALUES ({0}, {0})".format(1))

        # and replication still works
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
    assert_uses_zk_node(node, "zoo2")
    node.query_with_retry("INSERT INTO simple VALUES ({0}, {0})".format(2))

    # This is to double the replication logic.
    # TODO: add this, consider.
    # for node in [node2, node3]:
    #     assert (
    #         node.query_with_retry(
    #             "SELECT count() from simple",
    #             check_callback=lambda count: count.strip() == "2",
    #         )
    #         == "2\n"
    #     )
