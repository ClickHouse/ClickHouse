import multiprocessing
import pytest
from time import sleep
import random
from itertools import count
from sys import stdout

from multiprocessing.dummy import Pool

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry, assert_logs_contain
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


def test_create_keeper_map(started_cluster):
    node.query(
        "CREATE TABLE test_keeper_map (key UInt64, value UInt64) ENGINE = KeeperMap('/test1') PRIMARY KEY(key);"
    )
    zk_client = get_genuine_zk()

    def assert_children_size(path, expected_size):
        children_size = 0
        # 4 secs should be more than enough for replica to sync
        for _ in range(10):
            children_size = len(zk_client.get_children(path))
            if children_size == expected_size:
                return
            sleep(0.4)
        assert (
            False
        ), f"Invalid number of children for '{path}': actual {children_size}, expected {expected_size}"

    def assert_root_children_size(expected_size):
        assert_children_size("/test_keeper_map/test1", expected_size)

    def assert_data_children_size(expected_size):
        assert_children_size("/test_keeper_map/test1/data", expected_size)

    assert_root_children_size(2)
    assert_data_children_size(0)

    node.query("INSERT INTO test_keeper_map VALUES (1, 11)")
    assert_data_children_size(1)

    node.query(
        "CREATE TABLE test_keeper_map_another (key UInt64, value UInt64) ENGINE = KeeperMap('/test1') PRIMARY KEY(key);"
    )
    assert_root_children_size(2)
    assert_data_children_size(1)

    node.query("INSERT INTO test_keeper_map_another VALUES (1, 11)")
    assert_root_children_size(2)
    assert_data_children_size(1)

    node.query("INSERT INTO test_keeper_map_another VALUES (2, 22)")
    assert_root_children_size(2)
    assert_data_children_size(2)

    node.query("DROP TABLE test_keeper_map SYNC")
    assert_root_children_size(2)
    assert_data_children_size(2)

    node.query("DROP TABLE test_keeper_map_another SYNC")
    assert_root_children_size(0)

    zk_client.stop()


def create_drop_loop(index, stop_event):
    table_name = f"test_keeper_map_{index}"

    for i in count(0, 1):
        if stop_event.is_set():
            return

        node.query_with_retry(
            f"CREATE TABLE IF NOT EXISTS {table_name} (key UInt64, value UInt64) ENGINE = KeeperMap('/test') PRIMARY KEY(key);"
        )
        node.query_with_retry(f"INSERT INTO {table_name} VALUES ({index}, {i})")
        result = node.query_with_retry(
            f"SELECT value FROM {table_name} WHERE key = {index}"
        )
        assert result.strip() == str(i)
        node.query_with_retry(f"DROP TABLE IF EXISTS {table_name} SYNC")


def test_create_drop_keeper_map_concurrent(started_cluster):
    pool = Pool()
    manager = multiprocessing.Manager()
    stop_event = manager.Event()
    results = []
    for i in range(multiprocessing.cpu_count()):
        sleep(0.2)
        results.append(
            pool.apply_async(
                create_drop_loop,
                args=(
                    i,
                    stop_event,
                ),
            )
        )

    sleep(60)
    stop_event.set()

    for result in results:
        result.get()

    pool.close()

    client = get_genuine_zk()
    assert len(client.get_children("/test_keeper_map/test")) == 0
    client.stop()


def test_keeper_map_without_zk(started_cluster):
    def assert_keeper_exception_after_partition(query):
        with PartitionManager() as pm:
            pm.drop_instance_zk_connections(node)
            error = node.query_and_get_error(query)
            assert "Coordination::Exception" in error

    assert_keeper_exception_after_partition(
        "CREATE TABLE test_keeper_map_without_zk (key UInt64, value UInt64) ENGINE = KeeperMap('/test_without_zk') PRIMARY KEY(key);"
    )

    node.query(
        "CREATE TABLE test_keeper_map_without_zk (key UInt64, value UInt64) ENGINE = KeeperMap('/test_without_zk') PRIMARY KEY(key);"
    )

    assert_keeper_exception_after_partition(
        "INSERT INTO test_keeper_map_without_zk VALUES (1, 11)"
    )
    node.query("INSERT INTO test_keeper_map_without_zk VALUES (1, 11)")

    assert_keeper_exception_after_partition("SELECT * FROM test_keeper_map_without_zk")
    node.query("SELECT * FROM test_keeper_map_without_zk")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.restart_clickhouse(60)
        error = node.query_and_get_error("SELECT * FROM test_keeper_map_without_zk")
        assert "Failed to activate table because of connection issues" in error

    node.query("SELECT * FROM test_keeper_map_without_zk")

    client = get_genuine_zk()
    remove_children(client, "/test_keeper_map/test_without_zk")
    node.restart_clickhouse(60)
    error = node.query_and_get_error("SELECT * FROM test_keeper_map_without_zk")
    assert "Failed to activate table because of invalid metadata in ZooKeeper" in error

    node.query("DETACH TABLE test_keeper_map_without_zk")

    client.stop()
