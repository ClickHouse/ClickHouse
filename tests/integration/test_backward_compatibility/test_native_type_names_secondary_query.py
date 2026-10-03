"""A secondary query with both binary type flags works between a client and a server of different versions."""

import shlex

import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
new_node = cluster.add_instance("new_node")
old_node = cluster.add_instance(
    "old_node",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def secondary_query(client, server, query):
    return client.exec_in_container(
        [
            "bash",
            "-c",
            f"clickhouse client --host {server.name} --query_kind secondary_query"
            " --output_format_native_encode_types_in_binary_format 1"
            " --input_format_native_decode_types_in_binary_format 1"
            f" --query {shlex.quote(query)}",
        ]
    )


@pytest.mark.parametrize(
    "client, server",
    [(old_node, new_node), (new_node, old_node)],
    ids=["old_client_new_server", "new_client_old_server"],
)
def test_secondary_query_with_binary_types(start_cluster, client, server):
    server.query("DROP TABLE IF EXISTS t_05317 SYNC")
    server.query("CREATE TABLE t_05317 (x UInt64) ENGINE = MergeTree ORDER BY x")

    assert (
        secondary_query(client, server, "SELECT 42::UInt64 AS x, 'str' AS s")
        == "42\tstr\n"
    )
    secondary_query(client, server, "INSERT INTO t_05317 VALUES (8)")

    assert server.query("SELECT x FROM t_05317") == "8\n"
