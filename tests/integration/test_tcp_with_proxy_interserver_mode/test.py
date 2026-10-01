import socket

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/config.xml",
        "configs/config.d/disallow_interserver_mode.xml",
        "configs/config.d/session_log.xml",
    ],
)
# Relies on the default value of `tcp_with_proxy_allow_interserver_mode`.
node_default = cluster.add_instance(
    "node_default",
    main_configs=["configs/config.d/config.xml"],
)

USER_INTERSERVER_MARKER = " INTERSERVER SECRET "


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def encode_varuint(value):
    result = bytearray()
    while value >= 0x80:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def encode_string(value):
    if isinstance(value, str):
        value = value.encode()
    return encode_varuint(len(value)) + value


def build_interserver_hello(include_cluster):
    packet = b"".join(
        [
            encode_varuint(0),  # Client::Hello
            encode_string("test"),
            encode_varuint(24),
            encode_varuint(1),
            encode_varuint(54471),
            encode_string(""),  # default database
            encode_string(USER_INTERSERVER_MARKER),
            encode_string(""),  # password
        ]
    )

    if include_cluster:
        packet += encode_string("cluster_with_secret")
        packet += encode_string(b"A" * 32)

    return packet


def receive_response(instance, port, packet, proxy_header=b""):
    with socket.create_connection(
        (instance.ip_address, port), timeout=10
    ) as connection:
        connection.settimeout(10)
        connection.sendall(proxy_header + packet)
        try:
            return connection.recv(65536)
        except (ConnectionResetError, BrokenPipeError):
            return b""


PROXY_HEADER = b"PROXY TCP4 192.0.2.1 192.0.2.2 12345 9011\r\n"


def test_interserver_mode_is_rejected_on_tcp_with_proxy_port(started_cluster):
    # Do not send the cluster name and salt. The listener policy must reject the
    # marker immediately instead of entering the interserver handshake parser.
    response = receive_response(
        node, 9011, build_interserver_hello(False), PROXY_HEADER
    )

    assert response == b""
    assert node.contains_in_log(
        "Interserver mode is disabled for connections to tcp_with_proxy_port"
    )

    # The rejection is routed through `Session::onAuthenticationFailure`,
    # which emits a `LoginFailure` row into `system.session_log`. With
    # `auth_use_forwarded_address` enabled, the row must carry the client
    # address from the PROXY header rather than the address of the proxy.
    node.query("SYSTEM FLUSH LOGS session_log")
    assert (
        int(
            node.query(
                "SELECT count() FROM system.session_log "
                "WHERE type = 'LoginFailure' AND interface = 'TCP_Interserver' "
                "AND failure_reason LIKE '%Interserver mode is disabled for connections to tcp_with_proxy_port%' "
                "AND client_address = toIPv6('192.0.2.1')"
            )
        )
        > 0
    )


def test_interserver_mode_remains_allowed_on_tcp_port(started_cluster):
    response = receive_response(node, 9000, build_interserver_hello(True))

    # Server::Hello = 0. Authentication using the cluster secret happens when
    # the subsequent query is received; accepting Hello proves the listener
    # policy did not reject the regular native listener.
    assert response[:1] == b"\x00"


def test_interserver_mode_allowed_on_tcp_with_proxy_port_by_default(started_cluster):
    response = receive_response(
        node_default, 9011, build_interserver_hello(True), PROXY_HEADER
    )

    # Server::Hello = 0. Without `tcp_with_proxy_allow_interserver_mode` set,
    # the interserver marker must still be accepted on `tcp_with_proxy_port`.
    assert response[:1] == b"\x00"
    assert not node_default.contains_in_log(
        "Interserver mode is disabled for connections to tcp_with_proxy_port"
    )
