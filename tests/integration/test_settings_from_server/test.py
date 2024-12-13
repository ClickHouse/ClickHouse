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
node_no_send = cluster.add_instance(
    "node_no_send",
    main_configs=["configs/config.d/no_send.xml"],
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

    # Setting changed by server but changed back client command line.
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

    # send_settings_to_client = false
    res = node_no_send.query("select 42::UInt64 as x format JSON")
    assert '"x": "42"' in res, "should be quoted"
