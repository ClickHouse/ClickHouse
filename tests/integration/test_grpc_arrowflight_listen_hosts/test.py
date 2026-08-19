import pyarrow.flight
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

wildcard_node = cluster.add_instance(
    "wildcard_node",
    main_configs=["configs/wildcard_hosts.xml"],
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
    whole server down with it (Arrow Flight) or silently share the port with the first one (gRPC)."""
    assert wildcard_node.query("SELECT 1") == "1\n"

    assert (
        len(wildcard_node.grep_in_log("Listening for gRPC protocol").splitlines()) == 1
    )
    assert (
        len(
            wildcard_node.grep_in_log(
                "Listening for Arrow Flight compatibility protocol"
            ).splitlines()
        )
        == 1
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
    still report an Arrow Flight configuration error that happens before binding a socket."""
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
