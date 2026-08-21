import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node")
peer = cluster.add_instance("peer")


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
