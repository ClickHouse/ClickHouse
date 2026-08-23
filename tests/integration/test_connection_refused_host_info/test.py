import pytest

from helpers.cluster import ClickHouseCluster

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


def test_connection_refused_error_contains_address(started_cluster):
    # A non-loopback peer is required: refusals discovered after EINPROGRESS
    # take the poll()-based error path in Poco::Net::SocketImpl::connect.
    peer_ip = cluster.get_instance_ip("peer")
    error = node.query_and_get_error(
        f"SELECT * FROM url('http://{peer_ip}:1/', 'CSV', 's String') SETTINGS http_max_tries = 1"
    )
    assert "Connection refused" in error
    assert f"{peer_ip}:1" in error


def test_connection_refused_error_names_the_resolved_peer(started_cluster):
    # Requesting a hostname must report the address that was actually dialled, not the name from the
    # URL: with several records behind one name, only the resolved IP says which peer refused. The
    # hostname on its own would still make the assertion above pass, so this needs its own case.
    peer_ip = cluster.get_instance_ip("peer")
    error = node.query_and_get_error(
        "SELECT * FROM url('http://peer:1/', 'CSV', 's String') SETTINGS http_max_tries = 1"
    )
    assert "Connection refused" in error
    assert f"{peer_ip}:1" in error, f"expected the resolved peer address in: {error}"


def test_connection_refused_through_proxy_names_the_resolved_proxy(started_cluster):
    # An HTTPS request through an HTTP proxy is tunnelled, and the tunnel does its own connect to
    # the proxy. That connect must dial the address already resolved for this attempt and name it:
    # re-resolving the proxy name would report a record that never refused anything, and reporting
    # the configured name would say nothing about which record did. Only the resolved IP does, so
    # asserting on it is what keeps the tunnel from silently regressing to a bare proxyConnect().
    peer_ip = cluster.get_instance_ip("peer")
    error = proxy_node.query_and_get_error(
        "SELECT * FROM url('https://peer:443/', 'CSV', 's String') SETTINGS http_max_tries = 1"
    )
    assert "Connection refused" in error
    assert f"{peer_ip}:1" in error, f"expected the resolved proxy address in: {error}"
