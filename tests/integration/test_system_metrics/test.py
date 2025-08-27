import pytest

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
    main_configs=["configs/overrides.xml"],
    with_zookeeper=True,
    stay_alive=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/overrides.xml"], with_zookeeper=True
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
    try:
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
        bytes = int(
            node1.query(
                "SELECT value FROM system.metrics WHERE metric = 'StorageBufferBytes'"
            )
        )
        assert 24 <= bytes <= 25

        node1.query("INSERT INTO test.buffer_table VALUES('hello');")
        assert (
            node1.query(
                "SELECT value FROM system.metrics WHERE metric = 'StorageBufferRows'"
            )
            == "2\n"
        )
        bytes = int(
            node1.query(
                "SELECT value FROM system.metrics WHERE metric = 'StorageBufferBytes'"
            )
        )
        assert 24 <= bytes <= 25

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
    finally:
        node1.query(
            """
            DROP TABLE IF EXISTS test.test_mem_table SYNC;
            DROP TABLE IF EXISTS test.buffer_table SYNC;
            """
        )


def test_attach_without_zk_incr_readonly_metric(start_cluster):
    assert (
        node1.query("SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'")
        == "0\n"
    )

    try:
        tbl_uuid = node1.query("SELECT generateUUIDv4()").strip()
        node1.query(
            f"ATTACH TABLE test.test_no_zk UUID '{tbl_uuid}' (i Int8, d Date) ENGINE = ReplicatedMergeTree('no_zk_{tbl_uuid}', 'replica') ORDER BY tuple()"
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

        node1.query("DROP TABLE test.test_no_zk SYNC")
        assert_eq_with_retry(
            node1,
            "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'",
            "0\n",
            retry_count=300,
            sleep_time=1,
        )
    finally:
        node1.query("DROP TABLE IF EXISTS test.test_no_zk SYNC")


def test_broken_tables_readonly_metric(start_cluster):
    try:
        tbl_uuid = node1.query("SELECT generateUUIDv4()").strip()
        node1.query(
            f"CREATE TABLE test.broken_table_readonly(initial_name Int8) ENGINE = ReplicatedMergeTree('/clickhouse/broken_table_readonly_{tbl_uuid}', 'replica') ORDER BY tuple()"
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

        zk_client = cluster.get_kazoo_client("zoo1")

        columns_path = zk_path + "/columns"
        metadata = zk_client.get(columns_path)[0]
        modified_metadata = metadata.replace(b"initial_name", b"new_name")
        zk_client.set(columns_path, modified_metadata)

        node1.start_clickhouse()

        assert node1.contains_in_log(
            "Initialization failed, table will remain readonly"
        )
        assert (
            node1.query(
                "SELECT value FROM system.metrics WHERE metric = 'ReadonlyReplica'"
            )
            == "1\n"
        )
    finally:
        node1.query("DROP TABLE IF EXISTS test.broken_table_readonly SYNC")
