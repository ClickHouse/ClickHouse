import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance')


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    instance.query("CREATE USER OR REPLACE A")
    yield
    instance.query("DROP TABLE IF EXISTS table1")
    instance.query("DROP TABLE IF EXISTS table2")


def test_select_single_column():
    instance.query("CREATE TABLE table1(d DATE, a String, b UInt8) ENGINE = MergeTree ORDER BY d")

    select_query = "SELECT a FROM table1"
    assert "it's necessary to have grant SELECT(a) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(a) ON default.table1 TO A")
    assert instance.query(select_query, user = 'A') == ""

    instance.query("REVOKE SELECT(a) ON default.table1 FROM A")
    assert "it's necessary to have grant SELECT(a) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')


def test_select_single_column_with_table_grant():
    instance.query("CREATE TABLE table1(d DATE, a String, b UInt8) ENGINE = MergeTree ORDER BY d")

    select_query = "SELECT a FROM table1"
    assert "it's necessary to have grant SELECT(a) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT ON default.table1 TO A")
    assert instance.query(select_query, user = 'A') == ""

    instance.query("REVOKE SELECT(a) ON default.table1 FROM A")
    assert "it's necessary to have grant SELECT(a) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')


def test_select_all_columns():
    instance.query("CREATE TABLE table1(d DATE, a String, b UInt8) ENGINE = MergeTree ORDER BY d")

    select_query = "SELECT * FROM table1"
    assert "it's necessary to have grant SELECT(d, a, b) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(d) ON default.table1 TO A")
    assert "it's necessary to have grant SELECT(d, a, b) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(a) ON default.table1 TO A")
    assert "it's necessary to have grant SELECT(d, a, b) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(b) ON default.table1 TO A")
    assert instance.query(select_query, user = 'A') == ""


def test_select_all_columns_with_table_grant():
    instance.query("CREATE TABLE table1(d DATE, a String, b UInt8) ENGINE = MergeTree ORDER BY d")

    select_query = "SELECT * FROM table1"
    assert "it's necessary to have grant SELECT(d, a, b) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT ON default.table1 TO A")
    assert instance.query(select_query, user = 'A') == ""


def test_alias():
    instance.query("CREATE TABLE table1(x Int32, y Int32) ENGINE = MergeTree ORDER BY tuple()")

    select_query = "SELECT x, y, x + y AS s FROM table1"
    assert "it's necessary to have grant SELECT(x, y) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(x, y) ON default.table1 TO A")
    assert instance.query(select_query, user = 'A') == ""


def test_alias_columns():
    instance.query("CREATE TABLE table1(x Int32, y Int32, s Int32 ALIAS x + y) ENGINE = MergeTree ORDER BY tuple()")

    select_query = "SELECT * FROM table1"
    assert "it's necessary to have grant SELECT(x, y) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(x,y) ON default.table1 TO A")
    assert instance.query(select_query, user = 'A') == ""

    select_query = "SELECT s FROM table1"
    assert "it's necessary to have grant SELECT(s) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')
    
    instance.query("GRANT SELECT(s) ON default.table1 TO A")
    assert instance.query(select_query, user = 'A') == ""

    instance.query("REVOKE SELECT(x,y) ON default.table1 FROM A")
    assert instance.query(select_query, user = 'A') == ""


def test_materialized_columns():
    instance.query("CREATE TABLE table1(x Int32, y Int32, p Int32 MATERIALIZED x * y) ENGINE = MergeTree ORDER BY tuple()")

    select_query = "SELECT * FROM table1"
    assert "it's necessary to have grant SELECT(x, y) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(x,y) ON default.table1 TO A")
    assert instance.query(select_query, user = 'A') == ""

    select_query = "SELECT p FROM table1"
    assert "it's necessary to have grant SELECT(p) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')
    
    instance.query("GRANT SELECT(p) ON default.table1 TO A")
    assert instance.query(select_query, user = 'A') == ""

    instance.query("REVOKE SELECT(x,y) ON default.table1 FROM A")
    assert instance.query(select_query, user = 'A') == ""


def test_select_join():
    instance.query("CREATE TABLE table1(d DATE, a String, b UInt8) ENGINE = MergeTree ORDER BY d")
    instance.query("CREATE TABLE table2(d DATE, x UInt32, y UInt8) ENGINE = MergeTree ORDER BY d")

    select_query = "SELECT * FROM table1 JOIN table2 USING(d)"
    assert "it's necessary to have grant SELECT(d, x, y) ON default.table2" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(d, x, y) ON default.table2 TO A")
    assert "it's necessary to have grant SELECT(d, a, b) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(d, a, b) ON default.table1 TO A")
    assert instance.query(select_query, user = 'A') == ""

    instance.query("REVOKE SELECT ON default.table2 FROM A")
    assert "it's necessary to have grant SELECT(d, x, y) ON default.table2" in instance.query_and_get_error(select_query, user = 'A')


def test_select_union():
    instance.query("CREATE TABLE table1(a String, b UInt8) ENGINE = MergeTree ORDER BY tuple()")
    instance.query("CREATE TABLE table2(a String, b UInt8) ENGINE = MergeTree ORDER BY tuple()")

    select_query = "SELECT * FROM table1 UNION ALL SELECT * FROM table2"
    assert "it's necessary to have grant SELECT(a, b) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(a, b) ON default.table1 TO A")
    assert "it's necessary to have grant SELECT(a, b) ON default.table2" in instance.query_and_get_error(select_query, user = 'A')

    instance.query("GRANT SELECT(a, b) ON default.table2 TO A")
    assert instance.query(select_query, user = 'A') == ""

    instance.query("REVOKE SELECT ON default.table1 FROM A")
    assert "it's necessary to have grant SELECT(a, b) ON default.table1" in instance.query_and_get_error(select_query, user = 'A')
