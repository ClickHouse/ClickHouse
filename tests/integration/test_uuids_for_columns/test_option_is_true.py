import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


def create_params_in_zk(zk):
    zk.ensure_path("/clickhouse")


def zero_uuids(result):
    for uuid in result.rstrip().split("\n"):
        assert uuid == "00000000-0000-0000-0000-000000000000"


def non_zero_uuids(result):
    for uuid in result.rstrip().split("\n"):
        assert uuid != "00000000-0000-0000-0000-000000000000"


node_1 = cluster.add_instance(
    "node_1",
    main_configs=[
        "configs/clusters.xml",
        "configs/enable_uuids_for_columns_is_true.xml",
    ],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)

node_2 = cluster.add_instance(
    "node_2",
    main_configs=[
        "configs/clusters.xml",
        "configs/enable_uuids_for_columns_is_true.xml",
    ],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 2},
)

# node_3 does not belong to cluster
node_3 = cluster.add_instance(
    "node_3",
    main_configs=[
        "configs/clusters.xml",
        "configs/enable_uuids_for_columns_is_true.xml",
    ],
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


@pytest.mark.parametrize(
    "node",
    [node_1, node_3],
)
def test_atomic_create_table_on_cluster(started_cluster, node):
    """
    In Atomic database the columns created ON CLUSTER have the same non-zero UUIDs for CREATE TABLE and ADD COLUMN queries
    """
    node.query("DROP TABLE IF EXISTS atomic_cluster_table ON CLUSTER 'cluster'")
    node.query(
        "CREATE TABLE atomic_cluster_table ON CLUSTER 'cluster' (id UInt64, first_name String) ENGINE=MergeTree ORDER BY id"
    )
    node.query(
        "ALTER TABLE atomic_cluster_table ON CLUSTER 'cluster' ADD COLUMN address String"
    )
    result1 = node_1.query(
        "SELECT uuid FROM system.columns WHERE table='atomic_cluster_table'"
    )
    result2 = node_2.query(
        "SELECT uuid FROM system.columns WHERE table='atomic_cluster_table'"
    )
    non_zero_uuids(result1)
    assert result1 == result2


def test_memory_create_table_on_cluster(started_cluster):
    """
    In Memory database the columns created ON CLUSTER have the same zero UUIDs for CREATE TABLE and ADD COLUMN queries
    """
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
        "SELECT uuid FROM system.columns WHERE table='memory_cluster_table'"
    )
    result2 = node_2.query(
        "SELECT uuid FROM system.columns WHERE table='memory_cluster_table'"
    )
    zero_uuids(result1)
    assert result1 == result2


def test_atomic_create_table_on_single_node(started_cluster):
    """
    On non cluster queries in Atomic database the columns have non-zero UUIDs
    """
    node_1.query("DROP TABLE IF EXISTS atomic_single_table")
    node_1.query(
        "CREATE TABLE atomic_single_table (id UInt64, first_name String) ENGINE=MergeTree ORDER BY id"
    )
    node_1.query("ALTER TABLE atomic_single_table ADD COLUMN address String")
    result = node_1.query(
        "SELECT uuid FROM system.columns WHERE table='atomic_single_table'"
    )
    non_zero_uuids(result)

    # COLUMN_UUID is not displayed in create_table_query of system.tables
    result = node_1.query(
        "SELECT create_table_query FROM system.tables WHERE table='atomic_single_table'"
    )
    assert "COLUMN_UUID" not in result

    # COLUMN_UUID is not displayed in SHOW CREATE TABLE query
    result = node_1.query("SHOW CREATE TABLE atomic_single_table")
    assert "COLUMN_UUID" not in result

    # COLUMN_UUID is not displayed in DESCRIBE TABLE query
    result = node_1.query("DESCRIBE TABLE atomic_single_table")
    assert "COLUMN_UUID" not in result


def test_memory_create_table_on_single_node(started_cluster):
    """
    On non cluster queries in Memory database the columns have zero UUIDs
    """
    node_1.query("DROP DATABASE IF EXISTS db2")
    node_1.query("CREATE DATABASE db2 ENGINE = Memory")
    node_1.query(
        "CREATE TABLE db2.memory_single_table (id UInt64, first_name String) ENGINE=MergeTree ORDER BY id"
    )
    node_1.query("ALTER TABLE db2.memory_single_table ADD COLUMN address String")
    result = node_1.query(
        "SELECT uuid FROM system.columns WHERE table='memory_single_table'"
    )
    zero_uuids(result)


def test_show_table_system_columns(started_cluster):
    """
    uuid is displayed in the list of columns
    """
    result = node_1.query("SHOW CREATE TABLE system.columns")
    assert "`uuid` UUID," in result
