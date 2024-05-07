import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
# Version 21.6.3.14 has incompatible partition id for tables with UUID in partition key.
node_22_6 = cluster.add_instance(
    "node_22_6",
    image="clickhouse/clickhouse-server",
    tag="22.6",
    stay_alive=True,
    with_installed_binary=True,
    allow_analyzer=False,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_ip_types_binary_compatibility(start_cluster):
    node_22_6.query(
        "create table tab (ipv4 IPv4, ipv6 IPv6) engine = MergeTree order by tuple()"
    )
    node_22_6.query(
        "insert into tab values ('123.231.213.132', '0123:4567:89ab:cdef:fedc:ba98:7654:3210')"
    )
    res_22_6 = node_22_6.query("select * from tab")

    node_22_6.restart_with_latest_version()

    res_latest = node_22_6.query("select * from tab")

    assert res_22_6 == res_latest

    node_22_6.query("drop table tab")
