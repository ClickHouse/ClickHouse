import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Node without port_offset - uses the standard ports (tcp 9000, http 8123, ...).
node_default = cluster.add_instance(
    "node_default",
    main_configs=["configs/config.d/ports.xml"],
)

# Node with port_offset=100 - every configured port is shifted up by 100.
#
# The integration-test harness always connects to an instance on the native
# port 9000 (readiness check and clickhouse-client both hard-code it), so the
# offset node's *base* tcp_port is configured 100 below 9000. After the offset
# is applied it binds 9000 and stays reachable, which itself proves the offset
# was applied to the native port. The other base ports keep their standard
# values so the offset visibly shifts them: http 8123 -> 8223, mysql
# 9004 -> 9104, postgresql 9005 -> 9105.
node_offset = cluster.add_instance(
    "node_offset",
    main_configs=[
        "configs/config.d/ports_offset.xml",
        "configs/config.d/port_offset.xml",
    ],
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_port_offset_tcp(start_cluster):
    """The native (tcp) port is offset; both nodes are reachable on 9000."""
    # Default node uses tcp_port 9000 directly.
    assert node_default.query("SELECT 1").strip() == "1"
    # Offset node configures tcp_port 8900 + offset 100 = 9000, so it is
    # reachable on 9000 too. Being reachable at all proves the offset was
    # applied - otherwise it would be listening on 8900.
    assert node_offset.query("SELECT 1").strip() == "1"

    # tcpPort() reflects the offset port on both nodes.
    assert node_default.query("SELECT tcpPort()").strip() == "9000"
    assert node_offset.query("SELECT tcpPort()").strip() == "9000"


def test_port_offset_http(start_cluster):
    """The http port is offset: 8123 on the default node, 8223 on the offset node."""
    assert node_default.http_query("SELECT 1").strip() == "1"
    # Offset node: http_port 8123 + offset 100 = 8223.
    assert node_offset.http_query("SELECT 1", port=8223).strip() == "1"

    assert node_default.query("SELECT getServerPort('http_port')").strip() == "8123"
    assert node_offset.query("SELECT getServerPort('http_port')").strip() == "8223"


def test_port_offset_mysql(start_cluster):
    """The mysql port is offset: 9004 -> 9104."""
    assert node_default.query("SELECT getServerPort('mysql_port')").strip() == "9004"
    assert node_offset.query("SELECT getServerPort('mysql_port')").strip() == "9104"


def test_port_offset_postgresql(start_cluster):
    """The postgresql port is offset: 9005 -> 9105."""
    assert node_default.query("SELECT getServerPort('postgresql_port')").strip() == "9005"
    assert node_offset.query("SELECT getServerPort('postgresql_port')").strip() == "9105"


def test_port_offset_multiple_queries(start_cluster):
    """Both nodes can serve queries simultaneously on their respective ports."""
    node_default.query("DROP TABLE IF EXISTS test_table")
    node_default.query("CREATE TABLE test_table (id UInt32) ENGINE = Memory")
    node_default.query("INSERT INTO test_table VALUES (1), (2), (3)")

    node_offset.query("DROP TABLE IF EXISTS test_table")
    node_offset.query("CREATE TABLE test_table (id UInt32) ENGINE = Memory")
    node_offset.query("INSERT INTO test_table VALUES (10), (20), (30)")

    assert node_default.query("SELECT sum(id) FROM test_table").strip() == "6"
    assert node_offset.query("SELECT sum(id) FROM test_table").strip() == "60"

    node_default.query("DROP TABLE test_table")
    node_offset.query("DROP TABLE test_table")


def test_port_offset_system_tables(start_cluster):
    """system.server_settings exposes the configured port_offset."""
    offset_value = node_offset.query(
        "SELECT value FROM system.server_settings WHERE name = 'port_offset'"
    ).strip()
    assert offset_value == "100"

    default_offset = node_default.query(
        "SELECT value FROM system.server_settings WHERE name = 'port_offset'"
    ).strip()
    assert default_offset == "0"


def test_port_offset_client_explicit_port_not_offset(start_cluster):
    """An explicit client `--port` is the exact destination and is never shifted.

    The client configuration contains `port_offset`, but `--port 9000` must dial
    exactly 9000 (where the offset node listens: base 8900 + offset 100). If the
    offset were wrongly applied to the explicit port, the client would dial 9100
    and fail to connect.
    """
    node_offset.exec_in_container(
        [
            "bash",
            "-c",
            "echo '<config><port_offset>100</port_offset></config>' > /tmp/client_with_offset.xml",
        ]
    )
    result = node_offset.exec_in_container(
        [
            "clickhouse",
            "client",
            "--config-file=/tmp/client_with_offset.xml",
            "--port=9000",
            "--query=SELECT 1",
        ]
    )
    assert result.strip() == "1"

    # A port derived from `tcp_port` in the same configuration IS shifted:
    # 8900 + 100 = 9000, the port the server actually listens on.
    node_offset.exec_in_container(
        [
            "bash",
            "-c",
            "echo '<config><tcp_port>8900</tcp_port><port_offset>100</port_offset></config>'"
            " > /tmp/client_tcp_port_offset.xml",
        ]
    )
    result = node_offset.exec_in_container(
        [
            "clickhouse",
            "client",
            "--config-file=/tmp/client_tcp_port_offset.xml",
            "--query=SELECT 2",
        ]
    )
    assert result.strip() == "2"


def test_port_offset_clickhouse_local(start_cluster):
    """`clickhouse-local` shifts its listeners by `port_offset` too.

    The embedded client derives its port through the same configuration, so the
    listener and the client must move together: with tcp_port 7000 and offset 100
    the registered (bound) port is 7100. Runs inside the container, so the fixed
    port cannot collide with anything on the test host.
    """
    node_offset.exec_in_container(
        [
            "bash",
            "-c",
            "echo '<clickhouse><tcp_port>7000</tcp_port><port_offset>100</port_offset></clickhouse>'"
            " > /tmp/local_with_offset.xml",
        ]
    )
    result = node_offset.exec_in_container(
        [
            "clickhouse",
            "local",
            "--config-file=/tmp/local_with_offset.xml",
            "--query=SYSTEM START LISTEN TCP; SELECT getServerPort('tcp_port')",
        ]
    )
    assert result.strip() == "7100"


def test_port_offset_all_protocols(start_cluster):
    """All configured ports are offset on the offset node."""
    # (port_name, default node port, offset node port).
    # tcp_port lands back on 9000 (base 8900 + 100) so the harness stays connected.
    expected = [
        ("tcp_port", "9000", "9000"),
        ("http_port", "8123", "8223"),
        ("mysql_port", "9004", "9104"),
        ("postgresql_port", "9005", "9105"),
    ]

    for port_name, default_port, offset_port in expected:
        actual_default = node_default.query(
            f"SELECT getServerPort('{port_name}')"
        ).strip()
        assert (
            actual_default == default_port
        ), f"Default node {port_name}: expected {default_port}, got {actual_default}"

        actual_offset = node_offset.query(
            f"SELECT getServerPort('{port_name}')"
        ).strip()
        assert (
            actual_offset == offset_port
        ), f"Offset node {port_name}: expected {offset_port}, got {actual_offset}"
