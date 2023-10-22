import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_create_table_without_order_by():
    with pytest.raises(QueryRuntimeException):
        node.query("CREATE DATABASE IF NOT EXISTS test;")
        node.query("DROP TABLE IF EXISTS test.test_empty_order_by;")
        node.query(
            """
            CREATE TABLE test.test_empty_order_by
            (
                `a` UInt8,
            )
            ENGINE = MergeTree()
            SETTINGS index_granularity = 8192;
            """
        )

def test_create_table_without_order_by_and_setting_enabled():
    node.query("CREATE DATABASE IF NOT EXISTS test;")
    node.query("DROP TABLE IF EXISTS test.test_empty_order_by;")
    node.query("SET create_table_empty_primary_key_by_default = true;")
    node.query(
        """
        CREATE TABLE test.test_empty_order_by
        (
            `a` UInt8,
        )
        ENGINE = MergeTree()
        SETTINGS index_granularity = 8192;
        """
    )
    assert "ORDER BY tuple()" in node.query(f"SHOW CREATE TABLE test.test_empty_order_by;")

def test_create_table_without_order_by_and_setting_enabled_columwise_primary_key():
    node.query("CREATE DATABASE IF NOT EXISTS test;")
    node.query("DROP TABLE IF EXISTS test.test_empty_order_by;")
    node.query("SET create_table_empty_primary_key_by_default = true;")
    node.query(
        """
        CREATE TABLE test.test_empty_order_by
        (
            `a` UInt8 PRIMARY KEY,
            `b` String PRIMARY KEY
        )
        ENGINE = MergeTree()
        SETTINGS index_granularity = 8192;
        """
    )
    assert "ORDER BY (a, b)" in node.query(f"SHOW CREATE TABLE test.test_empty_order_by")