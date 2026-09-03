import socket
import time

import pyarrow.flight
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

wildcard_node = cluster.add_instance(
    "wildcard_node",
    main_configs=["configs/wildcard_hosts.xml"],
)

reload_node = cluster.add_instance(
    "reload_node",
    main_configs=["configs/reload_hosts.xml"],
)

unavailable_node = cluster.add_instance(
    "unavailable_node",
    main_configs=["configs/unavailable_host.xml"],
)

failed_cluster = ClickHouseCluster(__file__, name="all_unavailable")
all_unavailable_node = failed_cluster.add_instance(
    "all_unavailable_node",
    main_configs=["configs/all_unavailable_host.xml"],
)


WILDCARD_ADDRESSES = ("0.0.0.0", "::")


def decode_proc_net_address(hex_address):
    """Decode an address of `/proc/net/tcp` or `/proc/net/tcp6`: a hexadecimal dump of the raw
    address whose every 4-byte word is in host byte order. gRPC binds an `AF_INET6` socket for an
    IPv4 address, so an IPv4-mapped address is reported in its IPv4 form."""
    raw = bytes.fromhex(hex_address)
    raw = b"".join(raw[offset : offset + 4][::-1] for offset in range(0, len(raw), 4))
    family = socket.AF_INET if len(raw) == 4 else socket.AF_INET6
    address = socket.inet_ntop(family, raw)
    return address.removeprefix("::ffff:")


def listening_addresses(node, port):
    """The addresses of the sockets that listen on `port` inside the container, read from
    `/proc/net/tcp` and `/proc/net/tcp6` so that no extra tools are needed. Unlike the log, this
    observes the live listeners: a listener that was supposed to be replaced but is still around
    shows up here."""
    addresses = []
    for proc_file in ("/proc/net/tcp", "/proc/net/tcp6"):
        content = node.exec_in_container(["bash", "-c", f"cat {proc_file}"])
        for line in content.splitlines()[1:]:
            fields = line.split()
            local_address, state = fields[1], fields[3]
            if state != "0A":  # `TCP_LISTEN`
                continue
            hex_address, hex_port = local_address.rsplit(":", 1)
            if int(hex_port, 16) == port:
                addresses.append(decode_proc_net_address(hex_address))
    return sorted(addresses)


def is_single_wildcard(addresses):
    return len(addresses) == 1 and addresses[0] in WILDCARD_ADDRESSES


def is_closed(addresses):
    return not addresses


def wait_for_listeners(node, port, is_settled, description, timeout=30):
    """Wait until the sockets listening on `port` satisfy `is_settled`. A listener replaced by a
    reload is destroyed - and its socket closed - only after the new one has been created, so the
    settled state is what the test is about."""
    deadline = time.monotonic() + timeout
    while True:
        addresses = listening_addresses(node, port)
        if is_settled(addresses) or time.monotonic() > deadline:
            assert is_settled(
                addresses
            ), f"port {port}: {addresses} is not {description}"
            return
        time.sleep(0.5)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_one_grpc_listener_for_mixed_wildcard_listen_hosts():
    """gRPC binds a dual-stack socket for a wildcard `listen_host`, so it must replace any specific
    addresses that occur before or after it. A second listener would either fail to bind and take the
    whole server down with it (Arrow Flight) or silently share the port with the first one (gRPC).
    """
    assert wildcard_node.query("SELECT 1") == "1\n"

    assert (
        len(wildcard_node.grep_in_log("Listening for gRPC protocol").splitlines()) == 1
    )
    assert wildcard_node.contains_in_log("Listening for gRPC protocol: 0.0.0.0:9100")
    assert (
        len(
            wildcard_node.grep_in_log(
                "Listening for Arrow Flight compatibility protocol"
            ).splitlines()
        )
        == 1
    )
    assert wildcard_node.contains_in_log(
        "Listening for Arrow Flight compatibility protocol: 0.0.0.0:8888"
    )

    # That single listener must serve IPv4 too.
    wildcard_node.wait_until_port_is_ready(8888, timeout=10)
    client = pyarrow.flight.FlightClient(f"grpc+tcp://{wildcard_node.ip_address}:8888")
    try:
        table = client.do_get(pyarrow.flight.Ticket(b"SELECT 1")).read_all()
        assert table.column(0)[0].as_py() == 1
    finally:
        client.close()


def test_unavailable_listen_host_does_not_prevent_startup():
    """gRPC-based servers bind their socket when they are started rather than when they are created,
    so `listen_try` has to be honored at that point as well: a listener for an address that cannot be
    bound is dropped with a warning, and the server starts with the remaining ones."""
    assert unavailable_node.query("SELECT 1") == "1\n"

    assert unavailable_node.contains_in_log(
        "Failed to listen for gRPC protocol: 192.0.2.1:9100"
    )
    assert (
        len(unavailable_node.grep_in_log("Listening for gRPC protocol").splitlines())
        == 1
    )


def test_runtime_reload_normalizes_grpc_and_arrowflight_listen_hosts():
    """Reloading from a specific address to a wildcard must replace the existing gRPC and Arrow
    Flight listeners. Keeping the old listener while adding the wildcard one recreates the
    overlapping socket issue that startup normalization avoids."""
    assert reload_node.query("SELECT 1") == "1\n"
    assert len(reload_node.grep_in_log("Listening for gRPC protocol").splitlines()) == 1
    assert listening_addresses(reload_node, 9200) == [reload_node.ip_address]
    assert listening_addresses(reload_node, 8888) == [reload_node.ip_address]

    # The Arrow Flight port changes with the reload: `updateServers` stops the old listener but
    # destroys it only after `createServers` has bound the new one, and an Arrow Flight socket -
    # unlike a plain gRPC one - is not bound with `SO_REUSEPORT`, so it cannot be rebound on the
    # same port within a single reload.
    reload_node.exec_in_container(
        [
            "bash",
            "-c",
            """cat > /etc/clickhouse-server/config.d/reload_hosts.xml <<'EOF'
<clickhouse>
    <listen_host>reload_node</listen_host>
    <listen_host>0.0.0.0</listen_host>

    <listen_try>1</listen_try>

    <grpc_port>9200</grpc_port>
    <arrowflight_port>8889</arrowflight_port>
</clickhouse>
EOF""",
        ]
    )
    reload_node.query("SYSTEM RELOAD CONFIG")

    assert reload_node.query("SELECT 1") == "1\n"

    assert len(reload_node.grep_in_log("Listening for gRPC protocol").splitlines()) == 2
    assert reload_node.contains_in_log("Listening for gRPC protocol: 0.0.0.0:9200")
    assert (
        len(
            reload_node.grep_in_log(
                "Listening for Arrow Flight compatibility protocol"
            ).splitlines()
        )
        == 2
    )
    assert reload_node.contains_in_log(
        "Listening for Arrow Flight compatibility protocol: 0.0.0.0:8889"
    )

    # The log alone would also be satisfied by a broken reload that merely adds the wildcard
    # listener next to the old specific one, so check the live listeners: the specific address is
    # gone, and a single wildcard socket serves each port.
    wait_for_listeners(
        reload_node, 9200, is_single_wildcard, "a single wildcard listener"
    )
    wait_for_listeners(
        reload_node, 8889, is_single_wildcard, "a single wildcard listener"
    )
    wait_for_listeners(reload_node, 8888, is_closed, "closed")

    # The single Arrow Flight listener must serve IPv4 traffic on the new port.
    reload_node.wait_until_port_is_ready(8889, timeout=10)
    client = pyarrow.flight.FlightClient(f"grpc+tcp://{reload_node.ip_address}:8889")
    try:
        table = client.do_get(pyarrow.flight.Ticket(b"SELECT 1")).read_all()
        assert table.column(0)[0].as_py() == 1
    finally:
        client.close()


def test_start_listen_uses_the_reloaded_listen_hosts():
    """`SYSTEM START LISTEN` must recreate the listener from the current config rather than from
    the startup snapshot: after a reload has replaced the specific `listen_host` with a wildcard,
    a stop/start cycle must come back with the wildcard listener, not the original specific
    address."""
    # The same config as the reload test writes, so this test does not depend on it having run.
    reload_node.exec_in_container(
        [
            "bash",
            "-c",
            """cat > /etc/clickhouse-server/config.d/reload_hosts.xml <<'EOF'
<clickhouse>
    <listen_host>reload_node</listen_host>
    <listen_host>0.0.0.0</listen_host>

    <listen_try>1</listen_try>

    <grpc_port>9200</grpc_port>
    <arrowflight_port>8889</arrowflight_port>
</clickhouse>
EOF""",
        ]
    )
    reload_node.query("SYSTEM RELOAD CONFIG")
    wait_for_listeners(
        reload_node, 9200, is_single_wildcard, "a single wildcard listener"
    )

    # Stopping destroys the listener - `stopServers` erases the fully stopped server - so starting
    # recreates it from scratch and must pick up the wildcard `listen_host` of the reloaded config
    # instead of the specific address of the startup snapshot.
    reload_node.query("SYSTEM STOP LISTEN GRPC")
    wait_for_listeners(reload_node, 9200, is_closed, "closed")

    reload_node.query("SYSTEM START LISTEN GRPC")
    wait_for_listeners(
        reload_node, 9200, is_single_wildcard, "a single wildcard listener"
    )


def test_all_unavailable_listen_hosts_prevent_startup():
    """With no listener left after `listen_try` drops every gRPC listener, the server must not
    report readiness."""
    try:
        with pytest.raises(Exception):
            failed_cluster.start()

    finally:
        failed_cluster.shutdown()


def test_runtime_restart_reports_arrowflight_configuration_error():
    """`listen_try` only ignores unavailable listen addresses. A runtime listener restart must
    still report an Arrow Flight configuration error that happens before binding a socket.
    """
    wildcard_node.query("SYSTEM STOP LISTEN ARROW FLIGHT")
    wildcard_node.exec_in_container(
        [
            "bash",
            "-c",
            """cat > /etc/clickhouse-server/config.d/invalid_arrowflight_tls.xml <<'EOF'
<clickhouse>
    <arrowflight>
        <enable_ssl>1</enable_ssl>
        <ssl_cert_file>/nonexistent/certificate.pem</ssl_cert_file>
        <ssl_key_file>/nonexistent/key.pem</ssl_key_file>
    </arrowflight>
</clickhouse>
EOF""",
        ]
    )

    assert wildcard_node.query_and_get_error("SYSTEM RELOAD CONFIG")
    assert wildcard_node.query("SELECT 1") == "1\n"
