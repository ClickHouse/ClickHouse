import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance
from helpers.network import PartitionManager

cluster = ClickHouseCluster(
    __file__, zookeeper_config_path="configs/zookeeper_load_balancing.xml"
)

node1 = cluster.add_instance(
    "node1",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml", "configs/zookeeper_load_balancing.xml"],
)
node2 = cluster.add_instance(
    "node2",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml", "configs/zookeeper_load_balancing.xml"],
)
node3 = cluster.add_instance(
    "node3",
    with_zookeeper=True,
    main_configs=["configs/remote_servers.xml", "configs/zookeeper_load_balancing.xml"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        for node in [node1, node2, node3]:
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


def test_fallback_session(started_cluster: ClickHouseCluster):
    # only leave connecting to zoo3 possible
    with PartitionManager() as pm:
        for node in started_cluster.instances.values():
            for zk in ["zoo1", "zoo2"]:
                pm._add_rule(
                    {
                        "source": node.ip_address,
                        "destination": cluster.get_instance_ip(zk),
                        "action": "REJECT --reject-with tcp-reset",
                    }
                )

        for node in [node1, node2, node3]:
            # all nodes will have to switch to zoo3
            assert_uses_zk_node(node, "zoo3")

        node1.query_with_retry("INSERT INTO simple VALUES ({0}, {0})".format(1))

        # and replication still works
        for node in [node2, node3]:
            assert (
                node.query_with_retry(
                    "SELECT count() from simple",
                    check_callback=lambda count: count.strip() == "1",
                )
                == "1\n"
            )

    # at this point network partitioning has been reverted.
    # the nodes should switch to zoo1 because of `in_order` load-balancing.
    # otherwise they would connect to a random replica

    # but there's no reason to reconnect because current session works
    # and there's no "optimal" node with `in_order` load-balancing
    # so we need to break the current session

    for node in [node1, node2, node3]:
        assert_uses_zk_node(node, "zoo3")

    with PartitionManager() as pm:
        for node in started_cluster.instances.values():
            pm._add_rule(
                {
                    "source": node.ip_address,
                    "destination": cluster.get_instance_ip("zoo3"),
                    "action": "REJECT --reject-with tcp-reset",
                }
            )

        for node in [node1, node2, node3]:
            assert_uses_zk_node(node, "zoo1")

    node1.query_with_retry("INSERT INTO simple VALUES ({0}, {0})".format(2))
    for node in [node2, node3]:
        assert (
            node.query_with_retry(
                "SELECT count() from simple",
                check_callback=lambda count: count.strip() == "2",
            )
            == "2\n"
        )
