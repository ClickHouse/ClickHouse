"""An older server queries a newer shard with the binary type encoding of the Native format enabled."""

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

BINARY_TYPES = {
    "output_format_native_encode_types_in_binary_format": 1,
    "input_format_native_decode_types_in_binary_format": 1,
}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_old_initiator_new_shard(start_cluster):
    new_node.query("DROP TABLE IF EXISTS t_05317 SYNC")
    new_node.query(
        "CREATE TABLE t_05317 (x UInt64, s String) ENGINE = MergeTree ORDER BY x"
    )

    old_node.http_query(
        "INSERT INTO FUNCTION remote('new_node', default, t_05317) VALUES (42, 'str')",
        method="POST",
        params=BINARY_TYPES,
    )
    assert (
        old_node.http_query(
            "SELECT x, s FROM remote('new_node', default, t_05317)",
            params=BINARY_TYPES,
        )
        == "42\tstr\n"
    )
