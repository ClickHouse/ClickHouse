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


def test_connection_refused_through_multi_record_proxy_names_one_record(started_cluster):
    # The single-record proxy case above cannot notice the tunnel re-resolving the proxy name (two
    # resolutions in one process return the same record); with two A records behind the name, only
    # naming a concrete record proves the error reports an address that was really dialled. The
    # gtest ProxyTunnelDialsTheCallerResolvedAddress pins down deterministically which one that is.
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
    # The other cases here are refusals, which arrive as an immediate RST. This one covers the
    # deferred branch: the peer never answers, the kernel gives up on its own, and the socket is
    # left holding SO_ERROR = ETIMEDOUT.
    #
    # That code is the one entry in Poco's `SocketImpl::error` switch that throws without its `arg`,
    # so before the translation the client reported a bare "Timeout" naming nothing at all -- the
    # address in the message came only from the connection description appended afterwards. The
    # assertion below is on "connect timed out", which is what distinguishes the two: the address
    # alone would pass either way.
    peer_ip = cluster.get_instance_ip("peer")

    # The kernel has to lose this race for the deferred branch to be the one that reports, and it
    # cannot be hurried: /proc/sys is read-only in these containers (they get NET_ADMIN, not
    # --privileged), so `sysctl -w net.ipv4.tcp_syn_retries=...` is silently ignored. At the default
    # six retries the kernel gives up at roughly 1+2+4+...+64 = 127s, so the client timeout below is
    # set well beyond that. Without the margin ClickHouse's own deadline reports first, and the case
    # passes while never reaching the code it covers.

    with PartitionManager() as pm:
        # DROP rather than REJECT: a rejection would arrive as an RST and take the refused path.
        pm.partition_instances(node, peer, port=9000)
        error = node.query_and_get_error(
            f"SELECT * FROM remote('{peer_ip}:9000', system, one) SETTINGS "
            # Pinned, not left at the default: applySettingsQuirks turns async_socket_for_remote
            # off on some kernels unless it was explicitly changed, and the synchronous
            # SocketImpl::connect branch produces the same text -- so on those machines this would
            # pass without ever reaching the helper it is meant to cover.
            "async_socket_for_remote = 1, async_query_sending_for_remote = 1, "
            "connect_timeout_with_failover_ms = 300000, connections_with_failover_max_tries = 1"
        )

    assert "connect timed out" in error, f"expected the deferred-timeout text in: {error}"
    assert f"{peer_ip}:9000" in error, f"expected the dialled address in: {error}"
