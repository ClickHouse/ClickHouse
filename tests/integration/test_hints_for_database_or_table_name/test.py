import pytest
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_hints_for_database_name(started_cluster):
    node.query("""
        CREATE TABLE default.table_x (i Int64) ENGINE=Memory;
        INSERT INTO default.table_x SELECT 1;
        """)

    #with pytest.raises(QueryRuntimeException, match="DB::Exception: Database defaul doesn't exist. Maybe you meant: ['default']"):
    with pytest.raises(QueryRuntimeException, match="DB::Exception: Database defaul doesn't exist. Maybe you meant: \\['default'\\]"):
        node.query("SELECT * FROM defaul.table_y LIMIT 1;")
    assert int(node.query("SELECT count() FROM default.table_x;")) == 1
    node.query("DROP TABLE default.table_x;")


def test_hints_for_table_name(started_cluster):
    node.query("""
        CREATE TABLE default.table_x (i Int64) ENGINE=Memory;
        CREATE TABLE default.table_z (i Int64) ENGINE=Memory;
        INSERT INTO default.table_x SELECT 1;
        """)
    with pytest.raises(QueryRuntimeException, match="DB::Exception: Table default.table_y doesn't exist. Maybe you meant: \\['default.table_x'\\]"):
        node.query("""
            SELECT * FROM default.table_y LIMIT 1;
            """)
    assert int(node.query("SELECT count() FROM default.table_x;")) == 1
    node.query("""
            DROP TABLE default.table_x;
            DROP TABLE default.table_z;
            """)

