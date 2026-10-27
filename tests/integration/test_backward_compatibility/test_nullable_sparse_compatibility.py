# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
upstream_node = cluster.add_instance("upstream_node")
old_node = cluster.add_instance(
    "old_node",
    image="clickhouse/clickhouse-server",
    tag="25.9",
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_old_client_compatible(start_cluster):
    upstream_node.query(
        "DROP TABLE IF EXISTS test_sparse"
    )
    upstream_node.query(
        "CREATE TABLE test_sparse (n Nullable(UInt64)) ENGINE MergeTree ORDER BY () SETTINGS min_bytes_for_wide_part = 0, ratio_of_defaults_for_sparse_serialization = 0.1, serialization_info_version = 'with_types', nullable_serialization_version = 'allow_sparse'"
    )
    upstream_node.query(
        "INSERT INTO test_sparse SELECT null FROM numbers(100)",
    )
    old_node.query(
        f"SELECT * FROM remote('{upstream_node.ip_address}', default, test_sparse)",
    )
