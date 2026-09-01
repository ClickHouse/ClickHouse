import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")
peer = cluster.add_instance("peer")
proxy_node = cluster.add_instance("proxy_node", main_configs=["configs/proxy.xml"])
proxy_multi_node = cluster.add_instance(
    "proxy_multi_node", main_configs=["configs/proxy_multi.xml"]
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_connection_refused_error_contains_address(started_cluster):
    # Non-loopback peer: refusals discovered after EINPROGRESS take the poll()-based error path.
    peer_ip = cluster.get_instance_ip("peer")
    error = node.query_and_get_error(
        f"SELECT * FROM url('http://{peer_ip}:1/', 'CSV', 's String') SETTINGS http_max_tries = 1"
    )
    assert "Connection refused" in error
    assert f"{peer_ip}:1" in error


def test_connection_refused_error_names_the_resolved_peer(started_cluster):
    # Must report the resolved IP, not the hostname from the URL.
    peer_ip = cluster.get_instance_ip("peer")
    error = node.query_and_get_error(
        "SELECT * FROM url('http://peer:1/', 'CSV', 's String') SETTINGS http_max_tries = 1"
    )
    assert "Connection refused" in error
    assert f"{peer_ip}:1" in error, f"expected the resolved peer address in: {error}"


def test_connection_refused_through_proxy_names_the_resolved_proxy(started_cluster):
    # The tunnel's connect to the proxy must dial and name the resolved address.
    peer_ip = cluster.get_instance_ip("peer")
    error = proxy_node.query_and_get_error(
        "SELECT * FROM url('https://peer:443/', 'CSV', 's String') SETTINGS http_max_tries = 1"
    )
    assert "Connection refused" in error
    assert f"{peer_ip}:1" in error, f"expected the resolved proxy address in: {error}"


def test_connection_refused_through_multi_record_proxy_names_one_record(started_cluster):
    # With two A records, only naming a concrete record proves the error is real.
    peer_ip = cluster.get_instance_ip("peer")
    node_ip = cluster.get_instance_ip("node")
    proxy_multi_node.exec_in_container(
        [
            "bash",
            "-c",
            f"printf '{peer_ip} proxymulti\\n{node_ip} proxymulti\\n' >> /etc/hosts",
        ]
    )
    error = proxy_multi_node.query_and_get_error(
        "SELECT * FROM url('https://peer:443/', 'CSV', 's String') SETTINGS http_max_tries = 1"
    )
    assert "Connection refused" in error
    assert "proxymulti:1" not in error, f"the proxy must be named by address, not name: {error}"
    assert (
        f"{peer_ip}:1" in error or f"{node_ip}:1" in error
    ), f"expected a concrete proxy record in: {error}"


def test_connect_timeout_names_the_dialled_address(started_cluster):
    # Covers the deferred branch: peer never answers, kernel gives up, SO_ERROR = ETIMEDOUT.
    # Asserting on "connect timed out" distinguishes from the connection-description suffix.
    peer_ip = cluster.get_instance_ip("peer")

    # /proc/sys is read-only in these containers, so sysctl is ignored. At default 6 retries
    # the kernel gives up at ~127s; client timeout is set well beyond that.
    with PartitionManager() as pm:
        # DROP not REJECT: rejection arrives as RST and takes the refused path.
        pm.partition_instances(node, peer, port=9000)
        error = node.query_and_get_error(
            f"SELECT * FROM remote('{peer_ip}:9000', system, one) SETTINGS "
            # Pinned: applySettingsQuirks may turn async_socket_for_remote off on some kernels.
            "async_socket_for_remote = 1, async_query_sending_for_remote = 1, "
            "connect_timeout_with_failover_ms = 300000, connections_with_failover_max_tries = 1"
        )

    assert "connect timed out" in error, f"expected the deferred-timeout text in: {error}"
    assert f"{peer_ip}:9000" in error, f"expected the dialled address in: {error}"
