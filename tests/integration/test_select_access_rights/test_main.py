import re

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")


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
    instance.query(
        "CREATE TABLE table1(d DATE, a String, b UInt8) ENGINE = MergeTree ORDER BY d"
    )

    select_query = "SELECT a FROM table1"
    assert (
        "it's necessary to have the grant SELECT(a) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(a) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == ""

    instance.query("REVOKE SELECT(a) ON default.table1 FROM A")
    assert (
        "it's necessary to have the grant SELECT(a) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )


def test_select_single_column_with_table_grant():
    instance.query(
        "CREATE TABLE table1(d DATE, a String, b UInt8) ENGINE = MergeTree ORDER BY d"
    )

    select_query = "SELECT a FROM table1"
    assert (
        "it's necessary to have the grant SELECT(a) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT ON default.table1 TO A")
    assert instance.query(select_query, user="A") == ""

    instance.query("REVOKE SELECT(a) ON default.table1 FROM A")
    assert (
        "it's necessary to have the grant SELECT(a) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )


def test_select_all_columns():
    instance.query(
        "CREATE TABLE table1(d DATE, a String, b UInt8) ENGINE = MergeTree ORDER BY d"
    )

    select_query = "SELECT * FROM table1"
    assert (
        "it's necessary to have the grant SELECT(d, a, b) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(d) ON default.table1 TO A")
    assert (
        "it's necessary to have the grant SELECT(d, a, b) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(a) ON default.table1 TO A")
    assert (
        "it's necessary to have the grant SELECT(d, a, b) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(b) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == ""


def test_select_all_columns_with_table_grant():
    instance.query(
        "CREATE TABLE table1(d DATE, a String, b UInt8) ENGINE = MergeTree ORDER BY d"
    )

    select_query = "SELECT * FROM table1"
    assert (
        "it's necessary to have the grant SELECT(d, a, b) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT ON default.table1 TO A")
    assert instance.query(select_query, user="A") == ""


def test_alias():
    instance.query(
        "CREATE TABLE table1(x Int32, y Int32) ENGINE = MergeTree ORDER BY tuple()"
    )

    select_query = "SELECT x, y, x + y AS s FROM table1"
    assert (
        "it's necessary to have the grant SELECT(x, y) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(x, y) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == ""


def test_alias_columns():
    instance.query(
        "CREATE TABLE table1(x Int32, y Int32, s Int32 ALIAS x + y) ENGINE = MergeTree ORDER BY tuple()"
    )

    select_query = "SELECT * FROM table1"
    assert (
        "it's necessary to have the grant SELECT(x, y) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(x,y) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == ""

    select_query = "SELECT s FROM table1"
    assert (
        "it's necessary to have the grant SELECT(s) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(s) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == ""

    instance.query("REVOKE SELECT(x,y) ON default.table1 FROM A")
    assert instance.query(select_query, user="A") == ""


def test_materialized_columns():
    instance.query(
        "CREATE TABLE table1(x Int32, y Int32, p Int32 MATERIALIZED x * y) ENGINE = MergeTree ORDER BY tuple()"
    )

    select_query = "SELECT * FROM table1"
    assert (
        "it's necessary to have the grant SELECT(x, y) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(x,y) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == ""

    select_query = "SELECT p FROM table1"
    assert (
        "it's necessary to have the grant SELECT(p) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(p) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == ""

    instance.query("REVOKE SELECT(x,y) ON default.table1 FROM A")
    assert instance.query(select_query, user="A") == ""


def test_select_join():
    instance.query(
        "CREATE TABLE table1(d DATE, a String, b UInt8) ENGINE = MergeTree ORDER BY d"
    )
    instance.query(
        "CREATE TABLE table2(d DATE, x UInt32, y UInt8) ENGINE = MergeTree ORDER BY d"
    )

    select_query = "SELECT * FROM table1 JOIN table2 USING(d)"

    def match_error(err, columns, table):
        """Check if the error message contains the expected table and columns"""

        match = re.search(
            r"it's necessary to have the grant SELECT\((.*)\) ON default\.(\w+)", err
        )
        if not match:
            return False
        if match.group(2) != table:
            return False
        assert set(match.group(1).split(", ")) == set(
            columns.split(", ")
        ), f"expected {columns} in {err}"
        return True

    response = instance.query_and_get_error(select_query, user="A")
    table1_match = match_error(response, "d, a, b", "table1")
    table2_match = match_error(response, "d, x, y", "table2")
    assert table1_match or table2_match, response

    instance.query("GRANT SELECT(d, x, y) ON default.table2 TO A")
    response = instance.query_and_get_error(select_query, user="A")
    assert match_error(response, "d, a, b", "table1")

    response = instance.query_and_get_error(select_query, user="A")
    instance.query("GRANT SELECT(d, a, b) ON default.table1 TO A")

    assert instance.query(select_query, user="A") == ""

    instance.query("REVOKE SELECT ON default.table2 FROM A")
    response = instance.query_and_get_error(select_query, user="A")
    assert match_error(response, "d, x, y", "table2")


def test_select_union():
    instance.query(
        "CREATE TABLE table1(a String, b UInt8) ENGINE = MergeTree ORDER BY tuple()"
    )
    instance.query(
        "CREATE TABLE table2(a String, b UInt8) ENGINE = MergeTree ORDER BY tuple()"
    )

    select_query = "SELECT * FROM table1 UNION ALL SELECT * FROM table2"
    assert (
        "it's necessary to have the grant SELECT(a, b) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(a, b) ON default.table1 TO A")
    assert (
        "it's necessary to have the grant SELECT(a, b) ON default.table2"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(a, b) ON default.table2 TO A")
    assert instance.query(select_query, user="A") == ""

    instance.query("REVOKE SELECT ON default.table1 FROM A")
    assert (
        "it's necessary to have the grant SELECT(a, b) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )


def test_select_count():
    instance.query(
        "CREATE TABLE table1(x String, y UInt8) ENGINE = MergeTree ORDER BY tuple()"
    )

    select_query = "SELECT count() FROM table1"
    assert (
        "it's necessary to have the grant SELECT for at least one column on default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(x) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == "0\n"

    instance.query("REVOKE SELECT(x) ON default.table1 FROM A")
    assert (
        "it's necessary to have the grant SELECT for at least one column on default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(y) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == "0\n"

    instance.query("REVOKE SELECT(y) ON default.table1 FROM A")
    assert (
        "it's necessary to have the grant SELECT for at least one column on default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT ON default.table1 TO A")
    assert instance.query(select_query, user="A") == "0\n"


def test_select_where():
    # User should have grants for the columns used in WHERE.
    instance.query(
        "CREATE TABLE table1(a String, b UInt8) ENGINE = MergeTree ORDER BY b"
    )
    instance.query("INSERT INTO table1 VALUES ('xxx', 0), ('yyy', 1), ('zzz', 0)")
    instance.query("GRANT SELECT(a) ON default.table1 TO A")

    select_query = "SELECT a FROM table1 WHERE b = 0"
    assert (
        "it's necessary to have the grant SELECT(a, b) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(b) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == "xxx\nzzz\n"

    instance.query("REVOKE SELECT ON default.table1 FROM A")
    assert (
        "it's necessary to have the grant SELECT(a, b) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT ON default.table1 TO A")
    assert instance.query(select_query, user="A") == "xxx\nzzz\n"


def test_select_prewhere():
    # User should have grants for the columns used in PREWHERE.
    instance.query(
        "CREATE TABLE table1(a String, b UInt8) ENGINE = MergeTree ORDER BY b"
    )
    instance.query("INSERT INTO table1 VALUES ('xxx', 0), ('yyy', 1), ('zzz', 0)")
    instance.query("GRANT SELECT(a) ON default.table1 TO A")

    select_query = "SELECT a FROM table1 PREWHERE b = 0"
    assert (
        "it's necessary to have the grant SELECT(a, b) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT(b) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == "xxx\nzzz\n"

    instance.query("REVOKE SELECT ON default.table1 FROM A")
    assert (
        "it's necessary to have the grant SELECT(a, b) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("GRANT SELECT ON default.table1 TO A")
    assert instance.query(select_query, user="A") == "xxx\nzzz\n"


def test_select_with_row_policy():
    # Normal users should not aware of the existence of row policy filters.
    instance.query(
        "CREATE TABLE table1(a String, b UInt8) ENGINE = MergeTree ORDER BY b"
    )
    instance.query("INSERT INTO table1 VALUES ('xxx', 0), ('yyy', 1), ('zzz', 0)")
    instance.query("CREATE ROW POLICY pol1 ON table1 USING b = 0 TO A")

    select_query = "SELECT a FROM table1"
    select_query2 = "SELECT count() FROM table1"
    assert (
        "it's necessary to have the grant SELECT(a) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )
    assert (
        "it's necessary to have the grant SELECT for at least one column on default.table1"
        in instance.query_and_get_error(select_query2, user="A")
    )

    instance.query("GRANT SELECT(a) ON default.table1 TO A")
    assert instance.query(select_query, user="A") == "xxx\nzzz\n"
    assert instance.query(select_query2, user="A") == "2\n"

    instance.query("REVOKE SELECT(a) ON default.table1 FROM A")
    assert (
        "it's necessary to have the grant SELECT(a) ON default.table1"
        in instance.query_and_get_error(select_query, user="A")
    )
    assert (
        "it's necessary to have the grant SELECT for at least one column on default.table1"
        in instance.query_and_get_error(select_query2, user="A")
    )
