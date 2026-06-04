import pytest

from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# Version 21.6.3.14 has incompatible partition id for tables with UUID in partition key.
node = cluster.add_instance(
    "node",
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_ip_types_binary_compatibility(start_cluster):
    node.query(
        "create table tab (ipv4 IPv4, ipv6 IPv6) engine = MergeTree order by tuple()"
    )
    node.query(
        "insert into tab values ('123.231.213.132', '0123:4567:89ab:cdef:fedc:ba98:7654:3210')"
    )
    res_old = node.query("select * from tab")

    node.restart_with_latest_version()

    res_latest = node.query("select * from tab")

    assert res_old == res_latest

    node.query("drop table tab")
