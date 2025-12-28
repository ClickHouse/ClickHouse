import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/remote_servers.xml",
        "configs/config.d/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
    ],
    ipv4_address="10.5.96.11",
)
node_bind_host = cluster.add_instance(
    "node_bind_host",
    main_configs=["configs/config.d/remote_servers.xml"],
    ipv4_address="10.5.96.12",
)
node_bind_host_secure = cluster.add_instance(
    "node_bind_host_secure",
    main_configs=[
        "configs/config.d/remote_servers.xml",
        "configs/config.d/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
    ],
    ipv4_address="10.5.96.13",
)
node_bind_host_fail = cluster.add_instance(
    "node_bind_host_fail",
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
        # add extra ip address for bind_host in node_bind_host and node_bind_host_secure
        node_bind_host.exec_in_container(
            ["bash", "-c", "ip addr add 10.5.96.22/12 dev eth0"], privileged=True, user="root"
        )
        node_bind_host_secure.exec_in_container(
            ["bash", "-c", "ip addr add 10.5.96.23/12 dev eth0"], privileged=True, user="root"
        )
        yield cluster
    finally:
        # clean up
        for node in cluster.instances.values():
            node.query("DROP TABLE IF EXISTS mem")
        cluster.shutdown()


def test_bind_host(start_cluster):
    node_bind_host.query("""
        CREATE TABLE dist
        Engine=Distributed(test_cluster_bind_host, currentDatabase(), mem)
    """)
    assert node_bind_host.query("SELECT count() FROM dist") == "4\n"

    # clean up
    node_bind_host.query("DROP TABLE IF EXISTS dist")


def test_bind_host_secure(start_cluster):
    node_bind_host_secure.query("""
        CREATE TABLE dist
        Engine=Distributed(test_cluster_bind_host_secure, currentDatabase(), mem)
    """)
    assert node_bind_host_secure.query("SELECT count() FROM dist") == "4\n"

    # clean up
    node_bind_host_secure.query("DROP TABLE IF EXISTS dist")


def test_bind_host_fail(start_cluster):
    node_bind_host_fail.query("""
        CREATE TABLE dist
        Engine=Distributed(test_cluster_bind_host_fail, currentDatabase(), mem)
    """)
    # node can not be connected with wrong bind_host 1.2.3.4
    # Code: 210. DB::NetException: Net Exception: Cannot assign requested address: 1.2.3.4:0 (node:9000). (NETWORK_ERROR)
    with pytest.raises(Exception, match="ALL_CONNECTION_TRIES_FAILED"):
        node_bind_host_fail.query("SELECT count() FROM dist")
    # node_bind_host can be successfully connected using 10.5.96.14 without wrong bind_host 1.2.3.4
    assert node_bind_host_fail.query("SELECT count() FROM dist SETTINGS skip_unavailable_shards=1") == "2\n"

    # clean up
    node_bind_host_fail.query("DROP TABLE IF EXISTS dist")
