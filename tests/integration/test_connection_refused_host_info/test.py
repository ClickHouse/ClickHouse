import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/remote_servers.xml"])
peer = cluster.add_instance("peer", stay_alive=True)
proxy_node = cluster.add_instance("proxy_node", main_configs=["configs/proxy.xml"])
new_peer = cluster.add_instance("new_peer", stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_connection_refused_error_names_the_resolved_peer(started_cluster):
    # Non-loopback peer: the refusal arrives after `EINPROGRESS`, on the `poll` + `SO_ERROR` path.
    # The error must name the resolved IP, not the hostname from the URL.
    peer_ip = cluster.get_instance_ip("peer")
    error = node.query_and_get_error(
        "SELECT * FROM url('http://peer:1/', 'CSV', 's String') SETTINGS http_max_tries = 1"
    )
    assert "Connection refused" in error
    assert f"{peer_ip}:1" in error, f"expected the resolved peer address in: {error}"


def test_connection_refused_through_proxy_names_the_resolved_proxy(started_cluster):
    # The tunnel's connect to the proxy must name the resolved proxy address.
    peer_ip = cluster.get_instance_ip("peer")
    error = proxy_node.query_and_get_error(
        "SELECT * FROM url('https://peer:443/', 'CSV', 's String') SETTINGS http_max_tries = 1"
    )
    assert "Connection refused" in error
    assert f"{peer_ip}:1" in error, f"expected the resolved proxy address in: {error}"


def test_connect_timeout_names_the_peer(started_cluster):
    # Covers the deferred branch: peer never answers, kernel gives up, `SO_ERROR` = `ETIMEDOUT`.
    # "Timeout: <address>" is Poco's text; the address alone would also match the suffix.
    peer_ip = cluster.get_instance_ip("peer")

    # DROP makes the kernel report ETIMEDOUT after its default SYN retries (~127s), so the
    # client deadline must be longer and the async settings must bypass platform quirks.
    with PartitionManager() as pm:
        pm.partition_instances(node, peer, port=9000)
        error = node.query_and_get_error(
            f"SELECT * FROM remote('{peer_ip}:9000', system, one) SETTINGS "
            "async_socket_for_remote = 1, async_query_sending_for_remote = 1, "
            "connect_timeout_with_failover_ms = 300000, connections_with_failover_max_tries = 1"
        )

    assert (
        f"Timeout: {peer_ip}:9000" in error
    ), f"expected the peer address in: {error}"


def test_reconnect_error_uses_latest_peer(started_cluster):
    old_ip = cluster.get_instance_ip("peer")
    new_ip = cluster.get_instance_ip("new_peer")
    table = "test_reconnect_error_uses_latest_peer"

    node.query(
        f"CREATE TABLE {table} AS system.one "
        "ENGINE = Distributed(stale_endpoint_cluster, system, one)"
    )
    assert node.query(f"SELECT count() FROM {table}") == "1\n"

    try:
        peer.stop_clickhouse()
        new_peer.stop_clickhouse()
        node.exec_in_container(
            ["bash", "-c", f"echo '{new_ip} peer # stale-endpoint-test' >> /etc/hosts"]
        )
        node.query("SYSTEM DROP DNS CACHE")

        error = node.query_and_get_error(
            f"SELECT count() FROM {table} "
            "SETTINGS connections_with_failover_max_tries = 1"
        )
        assert f"Connection refused: {new_ip}:9000" in error
        assert error.count(f"{new_ip}:9000") == 1
        assert old_ip not in error
    finally:
        node.exec_in_container(
            [
                "bash",
                "-c",
                "sed '/# stale-endpoint-test$/d' /etc/hosts > /tmp/hosts.clean && cat /tmp/hosts.clean > /etc/hosts",
            ]
        )
        peer.start_clickhouse()
        new_peer.start_clickhouse()
        node.query(f"DROP TABLE IF EXISTS {table}")
