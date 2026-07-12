"""When neither `port` nor `secure`/`no-secure` is specified, clickhouse-client probes both the
plain (9000) and the secure (9440) native ports concurrently. The plain port is preferred when it
answers (backward compatibility), and TLS is enabled automatically when only the secure port is
reachable, e.g. for servers whose plain port is firewalled, like play.clickhouse.com."""

import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Serves both the plain and the secure native port (with a self-signed certificate).
node_both_ports = cluster.add_instance(
    "node_both_ports",
    main_configs=[
        "configs/ssl_config.xml",
        "certs/self-cert.pem",
        "certs/self-key.pem",
        "certs/ca-cert.pem",
    ],
)

# Serves only the plain port; also used to run the client from, so that the firewall rules
# on node_both_ports can be scoped to this instance's address and not break the test harness.
node_plain_only = cluster.add_instance("node_plain_only")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def firewall_plain_port(action):
    """Reject or drop packets from node_plain_only to the plain port of node_both_ports."""
    node_both_ports.exec_in_container(
        [
            "iptables",
            "--wait",
            "-A",
            "INPUT",
            "-p",
            "tcp",
            "--dport",
            "9000",
            "-s",
            node_plain_only.ip_address,
            "-j",
            action,
        ],
        user="root",
    )


def unfirewall_plain_port(action):
    node_both_ports.exec_in_container(
        [
            "iptables",
            "--wait",
            "-D",
            "INPUT",
            "-p",
            "tcp",
            "--dport",
            "9000",
            "-s",
            node_plain_only.ip_address,
            "-j",
            action,
        ],
        user="root",
    )


def redirect_plain_port_to_secure(add):
    """Redirect connections from node_plain_only to the plain port (9000) of node_both_ports
    to its secure port (9440). A native (non-TLS) connection to the plain port is then answered
    by the TLS listener, simulating a proxy that accepts TCP on the plain port but only serves
    TLS there. The TCP connection succeeds (so the probe prefers the plain port), but the native
    handshake fails."""
    node_both_ports.exec_in_container(
        [
            "iptables",
            "--wait",
            "-t",
            "nat",
            "-A" if add else "-D",
            "PREROUTING",
            "-p",
            "tcp",
            "--dport",
            "9000",
            "-s",
            node_plain_only.ip_address,
            "-j",
            "REDIRECT",
            "--to-ports",
            "9440",
        ],
        user="root",
    )


def run_client(server, *args, from_node=None, nothrow=False):
    from_node = from_node or node_plain_only
    return from_node.exec_in_container(
        ["clickhouse", "client", "--host", server.name, "--accept-invalid-certificate"]
        + list(args),
        nothrow=nothrow,
    )


def query_is_secure(server, *args, from_node=None):
    """Runs `SELECT 1` through the client without specifying a port and returns whether the
    connection was established over TLS."""
    query_id = str(uuid.uuid4())
    result = run_client(
        server,
        "--query_id",
        query_id,
        "--query",
        "SELECT 1",
        *args,
        from_node=from_node,
    )
    assert result == "1\n"
    server.query("SYSTEM FLUSH LOGS query_log")
    return int(
        server.query(
            f"SELECT is_secure FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish' LIMIT 1"
        )
    )


def test_plain_port_preferred_when_both_listen():
    assert query_is_secure(node_both_ports) == 0


def test_plain_port_preferred_when_only_plain_listens():
    assert query_is_secure(node_plain_only, from_node=node_both_ports) == 0


def test_explicit_secure_still_works():
    assert query_is_secure(node_both_ports, "--secure") == 1


def test_secure_port_chosen_when_plain_rejected():
    firewall_plain_port("REJECT")
    try:
        assert query_is_secure(node_both_ports) == 1
    finally:
        unfirewall_plain_port("REJECT")


def test_secure_port_chosen_when_plain_dropped():
    # Packets to the plain port silently disappear, as with a typical cloud firewall
    # (this is how play.clickhouse.com behaves). The client must not wait for the plain
    # connection attempt to time out: the ports are probed concurrently, so the whole
    # query has to finish well within the 10 seconds connect timeout.
    firewall_plain_port("DROP")
    try:
        start = time.time()
        assert query_is_secure(node_both_ports) == 1
        assert time.time() - start < 8
    finally:
        unfirewall_plain_port("DROP")


def test_secure_port_chosen_when_plain_serves_tls():
    # A proxy in front of the server accepts TCP on the plain port but only speaks TLS there.
    # The probe sees the plain port accept the connection and prefers it, but the native handshake
    # reads the TLS alert record as an unexpected packet (UNEXPECTED_PACKET_FROM_SERVER). The client
    # must treat that as a connection-level failure and retry over TLS on the secure port instead of
    # giving up, otherwise the "plain port accepts TCP but only serves TLS" case stays broken.
    redirect_plain_port_to_secure(add=True)
    try:
        assert query_is_secure(node_both_ports) == 1
    finally:
        redirect_plain_port_to_secure(add=False)


def test_explicit_port_is_not_upgraded():
    # With an explicit port or an explicit `no-secure` there is no automatic choice.
    firewall_plain_port("REJECT")
    try:
        for extra_args in ("--port 9000", "--no-secure"):
            output = node_plain_only.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"clickhouse client --host {node_both_ports.name} {extra_args} --query 'SELECT 1' 2>&1 || true",
                ]
            )
            assert output.strip() != "1"
            assert "refused" in output.lower()
    finally:
        unfirewall_plain_port("REJECT")
