import pytest
import time

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

test_recover_staled_replica_run = 1

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper_map.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def get_genuine_zk():
    return cluster.get_kazoo_client("zoo1")


def remove_children(client, path):
    children = client.get_children(path)

    for child in children:
        child_path = f"{path}/{child}"
        remove_children(client, child_path)
        client.delete(child_path)


def test_keeper_map_without_zk(started_cluster):
    def wait_disconnect_from_zk():
        for _ in range(20):
            if len(node.query_and_get_answer_with_error("SELECT * FROM system.zookeeper WHERE path='/'")[1]) != 0:
                break
            time.sleep(1)
        else:
            assert False, "ClickHouse didn't disconnect from ZK after DROP rule was added"

    def assert_keeper_exception_after_partition(query):
        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(node)
            wait_disconnect_from_zk()
            error = node.query_and_get_error(query)
            assert "Coordination::Exception" in error

    assert_keeper_exception_after_partition(
        "CREATE TABLE test_keeper_map_without_zk (key UInt64, value UInt64) ENGINE = KeeperMap('/test_without_zk') PRIMARY KEY(key);"
    )

    node.query_with_retry(
        "CREATE TABLE test_keeper_map_without_zk (key UInt64, value UInt64) ENGINE = KeeperMap('/test_without_zk') PRIMARY KEY(key);"
    )

    assert_keeper_exception_after_partition(
        "INSERT INTO test_keeper_map_without_zk VALUES (1, 11)"
    )
    node.query_with_retry("INSERT INTO test_keeper_map_without_zk VALUES (1, 11)")

    assert_keeper_exception_after_partition("SELECT * FROM test_keeper_map_without_zk")
    node.query_with_retry("SELECT * FROM test_keeper_map_without_zk")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.restart_clickhouse(60)
        error = node.query_and_get_error("SELECT * FROM test_keeper_map_without_zk")
        assert "Failed to activate table because of connection issues" in error

    node.query_with_retry("SELECT * FROM test_keeper_map_without_zk")

    client = get_genuine_zk()
    remove_children(client, "/test_keeper_map/test_without_zk")
    node.restart_clickhouse(60)
    error = node.query_and_get_error("SELECT * FROM test_keeper_map_without_zk")
    assert "Failed to activate table because of invalid metadata in ZooKeeper" in error

    node.query("DETACH TABLE test_keeper_map_without_zk")

    client.stop()
