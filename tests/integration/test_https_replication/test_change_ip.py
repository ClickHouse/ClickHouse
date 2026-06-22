import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

"""
Both ssl_conf.xml and no_ssl_conf.xml have the same port
"""


def _fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
            """
                CREATE DATABASE test;
    
                CREATE TABLE test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{shard}/replicated', '{replica}') PARTITION BY toYYYYMM(date) ORDER BY id;
            """.format(
                shard=shard, replica=node.name
            )
        )


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/remote_servers.xml",
        "configs/listen_host.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/dhparam.pem",
    ],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1111",
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/remote_servers.xml",
        "configs/listen_host.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/dhparam.pem",
    ],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1112",
)


@pytest.fixture(scope="module")
def both_https_cluster():
    try:
        cluster.start()

        _fill_nodes([node1, node2], 1)

        yield cluster

    finally:
        cluster.shutdown()


def test_replication_when_node_ip_changed(both_https_cluster):
    """
    Test for a bug when replication over HTTPS stops working when the IP of the source replica was changed.

    node1 is a source node
    node2 fethes data from node1
    """
    node1.query("truncate table test_table")
    node2.query("truncate table test_table")

    # First we check, that normal replication works
    node1.query(
        "INSERT INTO test_table VALUES ('2022-10-01', 1), ('2022-10-02', 2), ('2022-10-03', 3)"
    )
    assert node1.query("SELECT count(*) from test_table") == "3\n"
    assert_eq_with_retry(node2, "SELECT count(*) from test_table", "3")

    # We change source node ip
    cluster.restart_instance_with_ip_change(node1, "2001:3984:3989::1:7777")

    # Put some data to source node1
    node1.query(
        "INSERT INTO test_table VALUES ('2018-10-01', 4), ('2018-10-02', 4), ('2018-10-03', 6)"
    )
    # Check that data is placed on node1
    assert node1.query("SELECT count(*) from test_table") == "6\n"

    # drop DNS cache
    node2.query("SYSTEM DROP DNS CACHE")
    # Data is fetched
    assert_eq_with_retry(node2, "SELECT count(*) from test_table", "6")
