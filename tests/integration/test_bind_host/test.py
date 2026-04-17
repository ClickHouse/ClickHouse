import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/config.d/remote_servers.xml",
        "configs/config.d/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
    ],
    ipv4_address="10.5.96.11",
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.d/remote_servers.xml"],
    ipv4_address="10.5.96.12",
)
node3 = cluster.add_instance(
    "node3",
    main_configs=[
        "configs/config.d/remote_servers.xml",
        "configs/config.d/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
    ],
    ipv4_address="10.5.96.13",
)
node4 = cluster.add_instance(
    "node4",
    main_configs=["configs/config.d/remote_servers.xml"],
    ipv4_address="10.5.96.14",
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        # create local table for Distributed on all nodes
        for node in cluster.instances.values():
            node.query("DROP TABLE IF EXISTS mem")
            node.query("CREATE TABLE mem (key Int) Engine=Memory()")
            node.query("INSERT INTO mem VALUES (1), (2)")
        # add extra ip address for bind_host in node2 and node3
        node2.exec_in_container(
            ["bash", "-c", "ip addr add 10.5.96.22/12 dev eth0"], privileged=True, user="root"
        )
        node3.exec_in_container(
            ["bash", "-c", "ip addr add 10.5.96.23/12 dev eth0"], privileged=True, user="root"
        )
        yield cluster
    finally:
        # clean up
        for node in cluster.instances.values():
            node.query("DROP TABLE IF EXISTS mem")
        cluster.shutdown()


def test_bind_host(start_cluster):
    node2.query("DROP TABLE IF EXISTS dist")
    node2.query("""
        CREATE TABLE dist
        Engine=Distributed(test_cluster_bind_host, currentDatabase(), mem)
    """)
    assert node2.query("SELECT count() FROM dist") == "4\n"


def test_bind_host_secure(start_cluster):
    node3.query("DROP TABLE IF EXISTS dist")
    node3.query("""
        CREATE TABLE dist
        Engine=Distributed(test_cluster_bind_host_secure, currentDatabase(), mem)
    """)
    assert node3.query("SELECT count() FROM dist") == "4\n"


def test_bind_host_fail(start_cluster):
    node4.query("DROP TABLE IF EXISTS dist")
    node4.query("""
        CREATE TABLE dist
        Engine=Distributed(test_cluster_bind_host_fail, currentDatabase(), mem)
    """)
    # node1 can not be connected with wrong bind_host 1.2.3.4
    # Code: 210. DB::NetException: Net Exception: Cannot assign requested address: 1.2.3.4:0 (node1:9000). (NETWORK_ERROR)
    with pytest.raises(Exception, match="ALL_CONNECTION_TRIES_FAILED"):
        node4.query("SELECT count() FROM dist")

    # `skip_unavailable_shards=1` can skip the first shard (contains node1)
    # 2 rows: node2 can be successfully connected using 10.5.96.14 without wrong bind_host 1.2.3.4
    # 2 rows: node4 is the initiator node, so no need to send subquery over network because of localhost optimization
    assert node4.query("SELECT count() FROM dist SETTINGS skip_unavailable_shards=1") == "4\n"
