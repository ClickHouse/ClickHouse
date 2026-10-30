import itertools
import socket
import struct

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
# Interserver mode stays allowed on `tcp_with_proxy_port`, so the connection reaches the later rejections.
node_forwarded = cluster.add_instance(
    "node_forwarded",
    main_configs=[
        "configs/config.d/config.xml",
        "configs/config.d/use_forwarded_address.xml",
        "configs/config.d/session_log.xml",
    ],
)

USER_INTERSERVER_MARKER = " INTERSERVER SECRET "

# Below DBMS_MIN_PROTOCOL_VERSION_WITH_ADDENDUM (54458) and DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 (54462):
# no addendum and no nonce, so a Query packet can follow the Hello directly.
OLD_REVISION = 54449

# A distinct PROXY source port per connection identifies its `system.session_log` row.
source_ports = itertools.count(23001)


def proxy_header(source_port, source="192.0.2.1"):
    return f"PROXY TCP4 {source} 192.0.2.2 {source_port} 9011\r\n".encode()


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


def build_interserver_hello(
    include_cluster, cluster_name="cluster_with_secret", salt=b"A" * 32, revision=54471
):
    packet = b"".join(
        [
            encode_varuint(0),  # Client::Hello
            encode_string("test"),
            encode_varuint(24),
            encode_varuint(1),
            encode_varuint(revision),
            encode_string(""),  # default database
            encode_string(USER_INTERSERVER_MARKER),
            encode_string(""),  # password
        ]
    )

    if include_cluster:
        packet += encode_string(cluster_name)
        packet += encode_string(salt)

    return packet


def build_interserver_query(forwarded_for=None):
    if forwarded_for is None:
        interface = b"".join(
            [
                b"\x01",  # interface = TCP
                encode_string(""),  # os_user
                encode_string(""),  # client_hostname
                encode_string("test"),  # client_name
                encode_varuint(24),  # client_version_major
                encode_varuint(1),  # client_version_minor
                encode_varuint(OLD_REVISION),  # client_tcp_protocol_version
            ]
        )
    else:
        interface = b"".join(
            [
                b"\x02",  # interface = HTTP
                b"\x01",  # http_method = GET
                encode_string(""),  # http_user_agent
                encode_string(forwarded_for),  # forwarded_for
                encode_string(""),  # http_referer
            ]
        )

    return b"".join(
        [
            encode_varuint(1),  # Client::Query
            encode_string(""),  # query_id
            b"\x02",  # query_kind = SECONDARY_QUERY
            encode_string(""),  # initial_user
            encode_string(""),  # initial_query_id
            encode_string("127.0.0.1:9000"),  # initial_address
            struct.pack("<Q", 0),  # initial_query_start_time_microseconds
            interface,
            encode_string(""),  # quota_key
            encode_varuint(0),  # distributed_depth
            encode_varuint(0) if forwarded_for is None else b"",  # client_version_patch
            b"\x00",  # no OpenTelemetry trace context
            encode_string(""),  # end of settings
            encode_string(b"\x00" * 32),  # interserver hash, wrong
            encode_varuint(2),  # stage = Complete
            encode_varuint(0),  # compression disabled
            encode_string("SELECT 1"),  # query
        ]
    )


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


def send_and_wait_for_close(instance, packet):
    with socket.create_connection(
        (instance.ip_address, 9011), timeout=10
    ) as connection:
        connection.settimeout(10)
        connection.sendall(packet)
        try:
            while connection.recv(65536):
                pass
        except (ConnectionResetError, BrokenPipeError):
            pass


def count_login_failures(
    instance, failure_reason, source_port, client_address="192.0.2.1"
):
    instance.query("SYSTEM FLUSH LOGS session_log")
    failure_reason = failure_reason.replace("'", "\\'")
    return int(
        instance.query(
            "SELECT count() FROM system.session_log "
            "WHERE type = 'LoginFailure' AND interface = 'TCP_Interserver' "
            f"AND failure_reason = '{failure_reason}' AND client_port = {source_port} "
            f"AND client_address = toIPv6('{client_address}')"
        )
    )


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


def test_interserver_mode_is_rejected_with_malformed_forwarded_address(started_cluster):
    # `receiveProxyHeader` accepts a non-IP source token, and `auth_use_forwarded_address`
    # cannot parse it. The rejection must still be silent and recorded in `system.session_log`.
    proxy_header = b"PROXY TCP4 not_an_ip 192.0.2.2 12345 9011\r\n"
    response = receive_response(
        node, 9011, build_interserver_hello(False), proxy_header
    )

    assert response == b""

    node.query("SYSTEM FLUSH LOGS session_log")
    assert (
        int(
            node.query(
                "SELECT count() FROM system.session_log "
                "WHERE type = 'LoginFailure' AND interface = 'TCP_Interserver' "
                "AND failure_reason LIKE '%Interserver mode is disabled for connections to tcp_with_proxy_port%' "
                "AND client_address != toIPv6('192.0.2.1')"
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


def test_hello_reject_records_forwarded_address(started_cluster):
    # Unknown cluster: rejected in `receiveHello` before the handshake completes.
    port = next(source_ports)
    send_and_wait_for_close(
        node_forwarded,
        proxy_header(port)
        + build_interserver_hello(True, cluster_name="no_such_cluster"),
    )

    assert (
        count_login_failures(
            node_forwarded, "Requested cluster 'no_such_cluster' not found", port
        )
        > 0
    )


@pytest.mark.parametrize(
    "salt, forwarded_for, failure_reason",
    [
        pytest.param(
            b"",
            None,
            "Interserver authentication failed (no salt/cluster secret)",
            id="no_salt",
        ),
        # The client info of a Query packet is sent by the still unauthenticated peer, so its
        # X-Forwarded-For must not be taken for the client address.
        pytest.param(
            b"A" * 32,
            "198.51.100.7",
            "Interserver authentication failed",
            id="wrong_hash",
        ),
    ],
)
def test_query_reject_records_forwarded_address(
    started_cluster, salt, forwarded_for, failure_reason
):
    port = next(source_ports)
    send_and_wait_for_close(
        node_forwarded,
        proxy_header(port)
        + build_interserver_hello(True, salt=salt, revision=OLD_REVISION)
        + build_interserver_query(forwarded_for),
    )

    assert count_login_failures(node_forwarded, failure_reason, port) > 0
