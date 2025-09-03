import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[],
    user_configs=["configs/users.d/users.xml"],
)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_settings_from_server(started_cluster):
    # Setting changed by server (default user).
    res = node.query("select 42::UInt64 as x format JSON")
    assert '"x": 42' in res, "should be unquoted"

    # Setting changed by server to a different value (other user).
    res = node.query("select 42::UInt64 as x format JSON", user="second_user")
    assert '"x": "42"' in res, "should be quoted"

    # Setting not changed by server (default user).
    res = node.query("select 42::UInt64 as x format JSONEachRow")
    assert "[" not in res, "should not be formatted as a JSON array"

    # Setting changed by server (other user).
    res = node.query("select 42::UInt64 as x format JSONEachRow", user="second_user")
    assert "[" in res, "should be formatted as a JSON array"

    # Setting changed by server but changed back by the query.
    res = node.query(
        "select 42::UInt64 as x settings output_format_json_array_of_rows=0 format JSONEachRow",
        user="second_user",
    )
    assert "[" not in res, "should not be formatted as a JSON array"

    # Setting changed by server but changed back by client command line.
    res = node.query(
        "select 42::UInt64 as x format JSONEachRow",
        user="second_user",
        settings={"output_format_json_array_of_rows": "0"},
    )
    assert "[" not in res, "should not be formatted as a JSON array"

    # User created at runtime.
    node.query(
        "create user u identified with plaintext_password by '' settings date_time_output_format='unix_timestamp'"
    )
    res = node.query("select toDateTime64('1970-01-02 00:00:00', 0)", user="u")
    assert res == "86400\n"
    node.query("drop user u")

    # apply_settings_from_server = false
    res = node.query("select 42::UInt64 as x settings apply_settings_from_server=0 format JSON", user="second_user")
    assert '"x": 42' in res, "should be unquoted"

    # apply_settings_from_server = false using SET query.
    res = node.query("set apply_settings_from_server=0; select 42::UInt64 as x format JSON;", user="second_user")
    assert '"x": 42' in res, "should be quoted"

    # apply_settings_from_server = false in user profile.
    res = node.query("select 42::UInt64 as x format JSON", user="no_apply_user")
    assert '"x": 42' in res, "should be unquoted"

    # apply_settings_from_server = false received from server, unsuccessfully overridden by the query.
    res = node.query("select 42::UInt64 as x settings apply_settings_from_server=1 format JSON", user="no_apply_user")
    assert '"x": 42' in res, "should be unquoted"

    # apply_settings_from_server = false implicitly set using compatibility setting.
    res = node.query("select 42::UInt64 as x format JSON", user="compat_user")
    assert '"x": 42' in res, "should be quoted"

    # Multiple queries in one session with different value of apply_settings_from_server.
    res = node.query("select 42::UInt64 as x settings apply_settings_from_server=0 format JSON; select 42::UInt64 as x format JSON;", user="second_user")
    quoted_pos = res.find('"x": 42')
    unquoted_pos = res.find('"x": "42"')
    assert quoted_pos != -1, "first query should return quoted number"
    assert unquoted_pos != -1, "second query should return unquoted number"
    assert quoted_pos < unquoted_pos, "should be quoted for first query, unquoted for second"


@pytest.mark.parametrize("async_insert", [1, 0])
@pytest.mark.parametrize("user", ["default", "async_insert_user"])
def test_async_insert_from_server(started_cluster, async_insert, user):
    node.query("drop table if exists data")
    node.query("create table data (key Int) engine=Null")

    node.query(
        "insert into data values (1)",
        settings={"async_insert": async_insert},
        user=user,
    )

    node.query("drop table data")
