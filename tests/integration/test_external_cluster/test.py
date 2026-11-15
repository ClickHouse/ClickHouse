import re

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

control_node = cluster.add_instance(
    "control_node",
    main_configs=["configs/clusters.xml"],
    with_zookeeper=True,
)

data_node = cluster.add_instance(
    "data_node",
    main_configs=["configs/clusters.xml"],
    with_zookeeper=True,
    macros={"shard": 1, "replica": 1},
)

uuid_regex = re.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def assert_create_query(node, database_name, table_name, expected):
    replace_uuid = lambda x: re.sub(uuid_regex, "uuid", x)
    query = "SELECT create_table_query FROM system.tables WHERE database='{}' AND table='{}'".format(
        database_name, table_name
    )
    assert_eq_with_retry(node, query, expected, get_result=replace_uuid)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_ddl(started_cluster):
    control_node.query("CREATE DATABASE test_db ON CLUSTER 'external' ENGINE=Atomic")
    control_node.query(
        "CREATE TABLE test_db.test_table ON CLUSTER 'external' (id Int64) Engine=MergeTree ORDER BY id"
    )
    control_node.query(
        "ALTER TABLE test_db.test_table ON CLUSTER 'external' add column data String"
    )
    control_node.query("DETACH TABLE test_db.test_table ON CLUSTER 'external'")

    expected = ""
    assert_create_query(data_node, "test_db", "test_table", expected)

    control_node.query("ATTACH TABLE test_db.test_table ON CLUSTER 'external'")

    expected = "CREATE TABLE test_db.test_table (`id` Int64, `data` String) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192"
    assert_create_query(data_node, "test_db", "test_table", expected)

    control_node.query("DROP TABLE test_db.test_table ON CLUSTER 'external'")
    control_node.query("DROP DATABASE test_db ON CLUSTER 'external'")

    expected = ""
    assert_create_query(data_node, "test_db", "test_table", expected)


def test_ddl_replicated(started_cluster):
    control_node.query(
        "CREATE DATABASE test_db ON CLUSTER 'external' ENGINE=Replicated('/replicated')",
    )
    # Exception is expected
    assert "It's not initial query" in control_node.query_and_get_error(
        "CREATE TABLE test_db.test_table ON CLUSTER 'external' (id Int64) Engine=MergeTree ORDER BY id"
    )
    control_node.query("DROP DATABASE test_db ON CLUSTER 'external'")
