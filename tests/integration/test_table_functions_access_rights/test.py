import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance("instance")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()

        instance.query(
            "CREATE TABLE table1(x UInt32) ENGINE = MergeTree ORDER BY tuple()"
        )
        instance.query(
            "CREATE TABLE table2(x UInt32) ENGINE = MergeTree ORDER BY tuple()"
        )
        instance.query("INSERT INTO table1 VALUES (1)")
        instance.query("INSERT INTO table2 VALUES (2)")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        instance.query("DROP USER IF EXISTS A")


def test_merge():
    merge_spec = "merge('default', 'table[0-9]+')"
    select_query = f"SELECT * FROM {merge_spec} ORDER BY x"
    assert instance.query(select_query) == "1\n2\n"

    instance.query("CREATE USER A")

    assert "no tables satisfied provided regexp" in instance.query_and_get_error(
        select_query, user="A"
    )

    instance.query("GRANT SELECT ON default.table1 TO A")
    assert instance.query(select_query, user="A") == "1\n"

    instance.query("GRANT SELECT ON default.* TO A")
    assert instance.query(select_query, user="A") == "1\n2\n"

    instance.query("REVOKE SELECT ON default.table1 FROM A")
    assert instance.query(select_query, user="A") == "2\n"

    instance.query("REVOKE ALL ON default.* FROM A")
    instance.query("GRANT SELECT ON default.table1 TO A")
    instance.query("GRANT INSERT ON default.table2 TO A")
    assert (
        "it's necessary to have the grant SELECT(x) ON default.table2"
        in instance.query_and_get_error(select_query, user="A")
    )

    instance.query("REVOKE ALL ON default.* FROM A")
    describe_query = f"DESCRIBE TABLE {merge_spec}"
    assert (
        "Either there is no database, which matches regular expression `default`, or there are no tables in the database matches `default`, which fit tables expression: table[0-9]+"
        in instance.query_and_get_error(describe_query, user="A")
    )
    instance.query("GRANT SHOW TABLES ON default.table1 TO A")
    assert (
        "it's necessary to have the grant SHOW COLUMNS ON default.table1"
        in instance.query_and_get_error(describe_query, user="A")
    )
    instance.query("GRANT SHOW COLUMNS ON default.table1 TO A")
    assert instance.query(describe_query) == "x\tUInt32\t\t\t\t\t\n"


def test_view_if_permitted():
    assert (
        instance.query(
            "SELECT * FROM viewIfPermitted(SELECT * FROM table1 ELSE null('x UInt32'))"
        )
        == "1\n"
    )

    expected_error = "requires a SELECT query with the result columns matching a table function after 'ELSE'"
    assert expected_error in instance.query_and_get_error(
        "SELECT * FROM viewIfPermitted(SELECT * FROM table1 ELSE null('x Int32'))"
    )
    assert expected_error in instance.query_and_get_error(
        "SELECT * FROM viewIfPermitted(SELECT * FROM table1 ELSE null('y UInt32'))"
    )

    instance.query("CREATE USER A")
    assert (
        instance.query(
            "SELECT * FROM viewIfPermitted(SELECT * FROM table1 ELSE null('x UInt32'))",
            user="A",
        )
        == ""
    )

    instance.query("GRANT SELECT ON table1 TO A")
    assert (
        instance.query(
            "SELECT * FROM viewIfPermitted(SELECT * FROM table1 ELSE null('x UInt32'))",
            user="A",
        )
        == "1\n"
    )
