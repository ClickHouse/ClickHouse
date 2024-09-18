import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/config.xml"],
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


def test_startup_with_small_bg_pool(started_cluster):
    node.query(
        "CREATE TABLE replicated_table (k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/replicated_table', 'r1') ORDER BY k"
    )

    node.query("INSERT INTO replicated_table VALUES(20, 30)")

    def assert_values():
        assert node.query("SELECT * FROM replicated_table") == "20\t30\n"

    assert_values()
    node.restart_clickhouse(stop_start_wait_sec=10)
    assert_values()

    node.query("DROP TABLE replicated_table SYNC")


def test_startup_with_small_bg_pool_partitioned(started_cluster):
    node.query(
        "CREATE TABLE replicated_table_partitioned (k UInt64, i32 Int32) ENGINE=ReplicatedMergeTree('/clickhouse/replicated_table_partitioned', 'r1') ORDER BY k"
    )

    node.query("INSERT INTO replicated_table_partitioned VALUES(20, 30)")

    def assert_values():
        assert node.query("SELECT * FROM replicated_table_partitioned") == "20\t30\n"

    assert_values()
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node)
        node.restart_clickhouse(stop_start_wait_sec=300)
        assert_values()

    # check that we activate it in the end
    node.query_with_retry("INSERT INTO replicated_table_partitioned VALUES(20, 30)")

    node.query("DROP TABLE replicated_table_partitioned SYNC")
