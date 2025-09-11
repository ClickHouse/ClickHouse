import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


def create_params_in_zk(zk):
    zk.ensure_path("/clickhouse")


node_1 = cluster.add_instance(
    "node_1",
    main_configs=["configs/clusters.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)

node_2 = cluster.add_instance(
    "node_2",
    main_configs=["configs/clusters.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.add_zookeeper_startup_command(create_params_in_zk)
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_atomic_create_table_on_cluster(started_cluster):
    node_1.query("DROP TABLE IF EXISTS atomic_cluster_table ON CLUSTER 'cluster'")
    node_1.query(
        "CREATE TABLE atomic_cluster_table ON CLUSTER 'cluster' (id UInt64, first_name String) ENGINE=MergeTree ORDER BY id"
    )
    node_1.query(
        "ALTER TABLE atomic_cluster_table ON CLUSTER 'cluster' ADD COLUMN address String"
    )
    result1 = node_1.query(
        "SELECT * from system.columns where table='atomic_cluster_table'"
    )
    result2 = node_2.query(
        "SELECT * from system.columns where table='atomic_cluster_table'"
    )
    assert result1 == result2


def test_memory_create_table_on_cluster(started_cluster):
    node_1.query("DROP DATABASE IF EXISTS db1 ON CLUSTER 'cluster'")
    node_1.query("CREATE DATABASE db1 ENGINE = Memory")
    node_2.query("CREATE DATABASE db1 ENGINE = Memory")
    node_1.query(
        "CREATE TABLE db1.memory_cluster_table ON CLUSTER 'cluster' (id UInt64, first_name String) ENGINE=MergeTree ORDER BY id"
    )
    node_1.query(
        "ALTER TABLE db1.memory_cluster_table ON CLUSTER 'cluster' ADD COLUMN address String"
    )
    result1 = node_1.query(
        "SELECT * from system.columns where table='memory_cluster_table'"
    )
    result2 = node_2.query(
        "SELECT * from system.columns where table='memory_cluster_table'"
    )
    assert result1 == result2


def test_atomic_create_table_on_single_node(started_cluster):
    node_1.query("DROP TABLE IF EXISTS atomic_single_table")
    node_1.query(
        "CREATE TABLE atomic_single_table (id UInt64, first_name String) ENGINE=MergeTree ORDER BY id"
    )
    node_1.query("ALTER TABLE atomic_single_table ADD COLUMN address String")
    result = node_1.query(
        "SELECT uuid from system.columns where table='atomic_single_table'"
    )
    for uuid in result.rstrip().split("\n"):
        assert uuid != "00000000-0000-0000-0000-000000000000"


def test_memory_create_table_on_single_node(started_cluster):
    node_1.query("DROP DATABASE IF EXISTS db2")
    node_1.query("CREATE DATABASE db2 ENGINE = Memory")
    node_1.query(
        "CREATE TABLE db2.memory_single_table (id UInt64, first_name String) ENGINE=MergeTree ORDER BY id"
    )
    node_1.query("ALTER TABLE db2.memory_single_table ADD COLUMN address String")
    result = node_1.query(
        "SELECT uuid from system.columns where table='memory_single_table'"
    )
    for uuid in result.rstrip().split("\n"):
        assert uuid == "00000000-0000-0000-0000-000000000000"
