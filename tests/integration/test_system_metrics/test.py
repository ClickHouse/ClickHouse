import time

import pytest
from kazoo.client import KazooClient

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry


def fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
            """
                CREATE DATABASE test;

                CREATE TABLE test.test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{shard}/replicated', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date) 
                SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5,
                cleanup_delay_period=0, cleanup_delay_period_random_add=0, cleanup_thread_preferred_points_per_iteration=0;
            """.format(
                shard=shard, replica=node.name
            )
        )


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/remote_servers.xml"], with_zookeeper=True
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        fill_nodes([node1, node2], 1)

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_readonly_metrics(start_cluster):
    assert (
        node1.query("SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'")
        == "0\n"
    )

    with PartitionManager() as pm:
        ## make node1 readonly -> heal -> readonly -> heal -> detach table -> heal -> attach table
        pm.drop_instance_zk_connections(node1)
        assert_eq_with_retry(
            node1,
            "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
            "1\n",
            retry_count=300,
            sleep_time=1,
        )

        pm.heal_all()
        assert_eq_with_retry(
            node1,
            "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
            "0\n",
            retry_count=300,
            sleep_time=1,
        )

        pm.drop_instance_zk_connections(node1)
        assert_eq_with_retry(
            node1,
            "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
            "1\n",
            retry_count=300,
            sleep_time=1,
        )

        node1.query("DETACH TABLE test.test_table")
        assert "0\n" == node1.query(
            "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'"
        )

        pm.heal_all()
        node1.query("ATTACH TABLE test.test_table")
        assert_eq_with_retry(
            node1,
            "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
            "0\n",
            retry_count=300,
            sleep_time=1,
        )


# For LowCardinality-columns, the bytes for N rows is not N*size of 1 row.
def test_metrics_storage_buffer_size(start_cluster):
    node1.query(
        """
        CREATE TABLE test.test_mem_table
        (
            `str` LowCardinality(String)
        )
        ENGINE = Memory;

        CREATE TABLE test.buffer_table
        (
            `str` LowCardinality(String)
        )
        ENGINE = Buffer('test', 'test_mem_table', 1, 600, 600, 1000, 100000, 100000, 10000000);
    """
    )

    # before flush
    node1.query("INSERT INTO test.buffer_table VALUES('hello');")
    assert (
        node1.query(
            "SELECT value FROM system.metrics WHERE metric = 'StorageBufferRows'"
        )
        == "1\n"
    )
    # By the way, this metric does not count the LowCardinality's dictionary size.
    assert (
        node1.query(
            "SELECT value FROM system.metrics WHERE metric = 'StorageBufferBytes'"
        )
        == "1\n"
    )

    node1.query("INSERT INTO test.buffer_table VALUES('hello');")
    assert (
        node1.query(
            "SELECT value FROM system.metrics WHERE metric = 'StorageBufferRows'"
        )
        == "2\n"
    )
    assert (
        node1.query(
            "SELECT value FROM system.metrics WHERE metric = 'StorageBufferBytes'"
        )
        == "2\n"
    )

    # flush
    node1.query("OPTIMIZE TABLE test.buffer_table")
    assert (
        node1.query(
            "SELECT value FROM system.metrics WHERE metric = 'StorageBufferRows'"
        )
        == "0\n"
    )
    assert (
        node1.query(
            "SELECT value FROM system.metrics WHERE metric = 'StorageBufferBytes'"
        )
        == "0\n"
    )


def test_attach_without_zk_incr_readonly_metric(start_cluster):
    assert (
        node1.query("SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'")
        == "0\n"
    )

    node1.query(
        "ATTACH TABLE test.test_no_zk UUID 'a50b7933-59b2-49ce-8db6-59da3c9b4413' (i Int8, d Date) ENGINE = ReplicatedMergeTree('no_zk', 'replica') ORDER BY tuple()"
    )
    assert_eq_with_retry(
        node1,
        "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
        "1\n",
        retry_count=300,
        sleep_time=1,
    )

    node1.query("DETACH TABLE test.test_no_zk")
    assert_eq_with_retry(
        node1,
        "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
        "0\n",
        retry_count=300,
        sleep_time=1,
    )

    node1.query("ATTACH TABLE test.test_no_zk")
    assert_eq_with_retry(
        node1,
        "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
        "1\n",
        retry_count=300,
        sleep_time=1,
    )

    node1.query("SYSTEM RESTORE REPLICA test.test_no_zk")
    assert_eq_with_retry(
        node1,
        "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
        "0\n",
        retry_count=300,
        sleep_time=1,
    )

    node1.query("DROP TABLE test.test_no_zk")
    assert_eq_with_retry(
        node1,
        "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
        "0\n",
        retry_count=300,
        sleep_time=1,
    )


def get_zk(timeout=30.0):
    _zk_instance = KazooClient(
        hosts=cluster.get_instance_ip("zoo1") + ":2181", timeout=timeout
    )
    _zk_instance.start()
    return _zk_instance


def test_broken_tables_readonly_metric(start_cluster):
    node1.query(
        "CREATE TABLE test.broken_table_readonly(initial_name Int8) ENGINE = ReplicatedMergeTree('/clickhouse/broken_table_readonly', 'replica') ORDER BY tuple()"
    )
    assert_eq_with_retry(
        node1,
        "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
        "0\n",
        retry_count=300,
        sleep_time=1,
    )

    zk_path = node1.query(
        "SELECT replica_path FROM system.replicas WHERE table = 'broken_table_readonly'"
    ).strip()

    node1.stop_clickhouse()

    zk_client = get_zk()

    columns_path = zk_path + "/columns"
    metadata = zk_client.get(columns_path)[0]
    modified_metadata = metadata.replace(b"initial_name", b"new_name")
    zk_client.set(columns_path, modified_metadata)

    node1.start_clickhouse()

    assert node1.contains_in_log("Initialization failed, table will remain readonly")
    assert (
        node1.query("SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'")
        == "1\n"
    )
