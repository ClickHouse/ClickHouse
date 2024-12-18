import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager, _NetworkManager

test_recover_staled_replica_run = 1

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/enable_keeper_map.xml"],
    user_configs=["configs/keeper_retries.xml"],
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


def print_iptables_rules():
    print(f"iptables rules: {_NetworkManager.get().dump_rules()}")


def assert_keeper_exception_after_partition(query):
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        try:
            error = node.query_and_get_error_with_retry(
                query,
                sleep_time=1,
            )
            assert "Coordination::Exception" in error
        except:
            print_iptables_rules()
            raise


def run_query(query):
    try:
        result = node.query_with_retry(query, sleep_time=1)
        return result
    except:
        print_iptables_rules()
        raise


def test_keeper_map_without_zk(started_cluster):
    run_query("DROP TABLE IF EXISTS test_keeper_map_without_zk SYNC")
    assert_keeper_exception_after_partition(
        "CREATE TABLE test_keeper_map_without_zk (key UInt64, value UInt64) ENGINE = KeeperMap('/test_keeper_map_without_zk') PRIMARY KEY(key);"
    )

    run_query(
        "CREATE TABLE test_keeper_map_without_zk (key UInt64, value UInt64) ENGINE = KeeperMap('/test_keeper_map_without_zk') PRIMARY KEY(key);"
    )

    assert_keeper_exception_after_partition(
        "INSERT INTO test_keeper_map_without_zk VALUES (1, 11)"
    )
    run_query("INSERT INTO test_keeper_map_without_zk VALUES (1, 11)")

    assert_keeper_exception_after_partition("SELECT * FROM test_keeper_map_without_zk")
    assert run_query("SELECT * FROM test_keeper_map_without_zk") == "1\t11\n"

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.restart_clickhouse(60)
        try:
            error = node.query_and_get_error_with_retry(
                "SELECT * FROM test_keeper_map_without_zk",
                sleep_time=1,
            )
            assert "Failed to activate table because of connection issues" in error
        except:
            print_iptables_rules()
            raise

    run_query("SELECT * FROM test_keeper_map_without_zk")

    client = get_genuine_zk()
    remove_children(client, "/test_keeper_map/test_keeper_map_without_zk")
    node.restart_clickhouse(60)
    error = node.query_and_get_error_with_retry(
        "SELECT * FROM test_keeper_map_without_zk"
    )
    assert "Failed to activate table because of invalid metadata in ZooKeeper" in error

    client.stop()


def test_keeper_map_with_failed_drop(started_cluster):
    run_query("DROP TABLE IF EXISTS test_keeper_map_with_failed_drop SYNC")
    run_query("DROP TABLE IF EXISTS test_keeper_map_with_failed_drop_another SYNC")
    run_query(
        "CREATE TABLE test_keeper_map_with_failed_drop (key UInt64, value UInt64) ENGINE = KeeperMap('/test_keeper_map_with_failed_drop') PRIMARY KEY(key);"
    )

    run_query("INSERT INTO test_keeper_map_with_failed_drop VALUES (1, 11)")
    run_query("SYSTEM ENABLE FAILPOINT keepermap_fail_drop_data")
    node.query("DROP TABLE test_keeper_map_with_failed_drop SYNC")

    zk_client = get_genuine_zk()
    assert (
        zk_client.get("/test_keeper_map/test_keeper_map_with_failed_drop/data")
        is not None
    )

    run_query("SYSTEM DISABLE FAILPOINT keepermap_fail_drop_data")
    run_query(
        "CREATE TABLE test_keeper_map_with_failed_drop_another (key UInt64, value UInt64) ENGINE = KeeperMap('/test_keeper_map_with_failed_drop') PRIMARY KEY(key);"
    )
