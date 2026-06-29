# pylint: disable=line-too-long
# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name

import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
upstream_node = cluster.add_instance("upstream_node", use_old_analyzer=True)
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


def test_old_client_compatible(start_cluster):
    old_node.query("INSERT INTO FUNCTION null('foo String') VALUES ('foo')('bar')")
    old_node.query(
        "INSERT INTO FUNCTION null('foo String') VALUES ('foo')('bar')",
        host=upstream_node.ip_address,
    )


def test_new_client_compatible(start_cluster):
    upstream_node.query(
        "INSERT INTO FUNCTION null('foo String') VALUES ('foo')('bar')",
        host=old_node.ip_address,
    )
    upstream_node.query("INSERT INTO FUNCTION null('foo String') VALUES ('foo')('bar')")
