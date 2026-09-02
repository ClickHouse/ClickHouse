import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")
peer = cluster.add_instance("peer")
proxy_node = cluster.add_instance("proxy_node", main_configs=["configs/proxy.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_connection_refused_error_names_the_resolved_peer(started_cluster):
    # Non-loopback peer: the refusal arrives after EINPROGRESS, on the poll() + SO_ERROR path.
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


def test_connect_timeout_names_the_dialled_address(started_cluster):
    # Covers the deferred branch: peer never answers, kernel gives up, SO_ERROR = ETIMEDOUT.
    # "Timeout: <address>" is Poco's text; the address alone would also match the suffix.
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

    assert (
        f"Timeout: {peer_ip}:9000" in error
    ), f"expected the dialled address in: {error}"
