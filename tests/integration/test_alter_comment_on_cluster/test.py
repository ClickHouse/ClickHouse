import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

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


def assert_create_query(nodes, database_name, table_name, expected):
    query = "SELECT create_table_query FROM system.tables WHERE database='{}' AND table='{}'".format(
        database_name, table_name
    )
    for node in nodes:
        assert_eq_with_retry(node, query, expected)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_comment(started_cluster):
    node_1.query(
        "CREATE TABLE test_table ON CLUSTER 'cluster' (id Int64) ENGINE=ReplicatedMergeTree() ORDER BY id"
    )
    node_1.query(
        "ALTER TABLE test_table ON CLUSTER 'cluster' COMMENT COLUMN id 'column_comment_1'"
    )
    node_1.query(
        "ALTER TABLE test_table ON CLUSTER 'cluster' MODIFY COMMENT 'table_comment_1';"
    )

    expected = "CREATE TABLE default.test_table (`id` Int64 COMMENT \\'column_comment_1\\') ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\') ORDER BY id SETTINGS index_granularity = 8192 COMMENT \\'table_comment_1\\'"
    assert_create_query([node_1, node_2], "default", "test_table", expected)

    node_1.query(
        "ALTER TABLE test_table ON CLUSTER 'cluster' COMMENT COLUMN id 'column_comment_2'"
    )
    node_1.query(
        "ALTER TABLE test_table ON CLUSTER 'cluster' MODIFY COMMENT 'table_comment_2';"
    )

    expected = "CREATE TABLE default.test_table (`id` Int64 COMMENT \\'column_comment_2\\') ENGINE = ReplicatedMergeTree(\\'/clickhouse/tables/{uuid}/{shard}\\', \\'{replica}\\') ORDER BY id SETTINGS index_granularity = 8192 COMMENT \\'table_comment_2\\'"
    assert_create_query([node_1, node_2], "default", "test_table", expected)
    node_1.query("DROP TABLE test_table ON CLUSTER 'cluster'")
