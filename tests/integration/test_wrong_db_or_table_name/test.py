import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def start():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_wrong_database_name(start):
    node.query(
        """
        CREATE TABLE test.table_test (i Int64) ENGINE=Memory;
        INSERT INTO test.table_test SELECT 1;
        """
    )

    with pytest.raises(
            QueryRuntimeException,
            match="DB::Exception: Database tes doesn't exist. Maybe you wanted to type: ['test']",
    ):
        node.query("SELECT * FROM tes.table_test LIMIT 1;")
    assert int(node.query("SELECT count() FROM test.table_test;")) == 1
    node.query("DROP TABLE test.table_test;")


def test_wrong_table_name(started_cluster):
    node.query(
        """
        CREATE TABLE test.table_test (i Int64) ENGINE=Memory;
        CREATE TABLE test.table_test2 (i Int64) ENGINE=Memory;
        INSERT INTO test.table_test SELECT 1;
        """
    )
    with pytest.raises(
            QueryRuntimeException,
            match="DB::Exception: Table table_test1 doesn't exist. Maybe you wanted to type: ['table_test2']",
    ):
        node.query("SELECT * FROM test.table_test1 LIMIT 1;")
    assert int(node.query("SELECT count() FROM test.table_test;")) == 1
    node.query(
        """
        DROP TABLE test.table_test;
        DROP TABLE test.table_test2;
        """
    )
