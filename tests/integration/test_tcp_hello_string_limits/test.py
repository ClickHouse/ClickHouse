import socket
import struct
import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/handshake_timeout.xml"],
)

MAX_HELLO_STRING_SIZE = 64 * 1024
OVERSIZED = 1 * 1024 * 1024  # 1 MB — exceeds the 64 KB limit


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def encode_varuint(n):
    """Encode an integer as a VarUInt (7 bits per byte, MSB continuation)."""
    buf = bytearray()
    while n >= 0x80:
        buf.append((n & 0x7F) | 0x80)
        n >>= 7
    buf.append(n & 0x7F)
    return bytes(buf)


def encode_string(s):
    """Encode a string as VarUInt length prefix + UTF-8 bytes."""
    if isinstance(s, str):
        s = s.encode("utf-8")
    return encode_varuint(len(s)) + s


def encode_oversized_string(size):
    """Encode a string header claiming `size` bytes, followed by actual data.

    We only send a small amount of actual data — the server should reject
    the connection after reading the size prefix (before the full payload).
    """
    return encode_varuint(size) + b"A" * min(size, 4096)


def build_hello_packet(
    client_name=b"test",
    version_major=24,
    version_minor=1,
    tcp_protocol_version=54471,
    default_db=b"",
    user=b"default",
    password=b"",
    oversized_field=None,
    oversized_size=OVERSIZED,
):
    """Build a native protocol Hello packet.

    If `oversized_field` is set to one of "client_name", "default_db",
    "user", "password", the corresponding field will be replaced with
    an oversized string header.
    """
    parts = []

    # Packet type: Client::Hello = 0
    parts.append(encode_varuint(0))

    # client_name
    if oversized_field == "client_name":
        parts.append(encode_oversized_string(oversized_size))
    else:
        parts.append(encode_string(client_name))

    # version_major, version_minor, tcp_protocol_version
    parts.append(encode_varuint(version_major))
    parts.append(encode_varuint(version_minor))
    parts.append(encode_varuint(tcp_protocol_version))

    # default_db
    if oversized_field == "default_db":
        parts.append(encode_oversized_string(oversized_size))
    else:
        parts.append(encode_string(default_db))

    # user
    if oversized_field == "user":
        parts.append(encode_oversized_string(oversized_size))
    else:
        parts.append(encode_string(user))

    # password
    if oversized_field == "password":
        parts.append(encode_oversized_string(oversized_size))
    else:
        parts.append(encode_string(password))

    return b"".join(parts)


USER_INTERSERVER_MARKER = " INTERSERVER SECRET "


def build_interserver_hello_with_oversized_cluster(oversized_size=OVERSIZED):
    """Build a Hello packet that triggers the interserver path,
    then sends an oversized cluster name."""
    parts = []

    # Hello packet with interserver marker as user and empty password
    parts.append(encode_varuint(0))  # Client::Hello = 0
    parts.append(encode_string(b"test"))  # client_name
    parts.append(encode_varuint(24))  # version_major
    parts.append(encode_varuint(1))  # version_minor
    parts.append(encode_varuint(54471))  # tcp_protocol_version
    parts.append(encode_string(b""))  # default_db
    parts.append(encode_string(USER_INTERSERVER_MARKER.encode()))  # user
    parts.append(encode_string(b""))  # password (empty for interserver)

    # After the hello fields, the server calls processClusterNameAndSalt
    # which reads cluster (string) and salt (string, max 32 bytes)
    parts.append(encode_oversized_string(oversized_size))  # cluster — oversized

    return b"".join(parts)


def decode_varuint(sock):
    """Read a VarUInt from a socket."""
    result = 0
    shift = 0
    while True:
        byte = sock.recv(1)
        if not byte:
            raise ConnectionError("Connection closed while reading VarUInt")
        b = byte[0]
        result |= (b & 0x7F) << shift
        if (b & 0x80) == 0:
            break
        shift += 7
    return result


def skip_string(sock):
    """Read and discard a length-prefixed string from a socket."""
    length = decode_varuint(sock)
    remaining = length
    while remaining > 0:
        chunk = sock.recv(min(remaining, 65536))
        if not chunk:
            raise ConnectionError("Connection closed while reading string")
        remaining -= len(chunk)


def read_exact(sock, n):
    """Read exactly n bytes from a socket."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Connection closed while reading bytes")
        buf += chunk
    return buf


def skip_settings(sock):
    """Read and discard a serialized Settings block from a socket.

    Format: sequence of (String name, VarUInt flags, String value),
    terminated by an empty name (single 0x00 byte).
    """
    while True:
        name_len = decode_varuint(sock)
        if name_len == 0:
            break
        remaining = name_len
        while remaining > 0:
            chunk = sock.recv(min(remaining, 65536))
            if not chunk:
                raise ConnectionError("Connection closed while reading settings name")
            remaining -= len(chunk)
        decode_varuint(sock)  # flags
        skip_string(sock)  # value


def complete_hello_handshake(sock, tcp_protocol_version=54471):
    """Send a valid Hello packet and read the server's Hello response.

    Returns the socket ready for sending addendum or query packets.
    The Hello uses user=default with empty password (no auth needed
    for default user in test config).
    """
    # Send Hello
    hello = build_hello_packet(tcp_protocol_version=tcp_protocol_version)
    sock.sendall(hello)

    # Read server Hello response:
    # VarUInt packet_type (0 = Hello), String server_name,
    # VarUInt major, VarUInt minor, VarUInt revision
    packet_type = decode_varuint(sock)
    if packet_type == 2:
        # Exception packet — auth failure or other error
        raise Exception("Server returned exception during Hello")
    assert packet_type == 0, f"Expected Hello response (0), got {packet_type}"

    skip_string(sock)  # server_name
    decode_varuint(sock)  # version_major
    decode_varuint(sock)  # version_minor
    decode_varuint(sock)  # revision

    # Conditional fields based on protocol version
    if tcp_protocol_version >= 54471:  # DBMS_MIN_REVISION_WITH_VERSIONED_PARALLEL_REPLICAS_PROTOCOL
        decode_varuint(sock)  # parallel_replicas_protocol_version
    if tcp_protocol_version >= 54058:  # DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE
        skip_string(sock)  # server_timezone
    if tcp_protocol_version >= 54372:  # DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
        skip_string(sock)  # server_display_name
    if tcp_protocol_version >= 54401:  # DBMS_MIN_REVISION_WITH_VERSION_PATCH
        decode_varuint(sock)  # version_patch
    if tcp_protocol_version >= 54470:  # DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS
        skip_string(sock)  # proto_send_chunked
        skip_string(sock)  # proto_recv_chunked
    if tcp_protocol_version >= 54461:  # DBMS_MIN_PROTOCOL_VERSION_WITH_PASSWORD_COMPLEXITY_RULES
        num_rules = decode_varuint(sock)
        for _ in range(num_rules):
            skip_string(sock)  # original_pattern
            skip_string(sock)  # exception_message
    if tcp_protocol_version >= 54462:  # DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2
        read_exact(sock, 8)  # nonce (Int64)
    if tcp_protocol_version >= 54474:  # DBMS_MIN_REVISION_WITH_SERVER_SETTINGS
        skip_settings(sock)
    if tcp_protocol_version >= 54477:  # DBMS_MIN_REVISION_WITH_QUERY_PLAN_SERIALIZATION
        decode_varuint(sock)  # query_plan_serialization_version
    if tcp_protocol_version >= 54479:  # DBMS_MIN_REVISION_WITH_VERSIONED_CLUSTER_FUNCTION_PROTOCOL
        decode_varuint(sock)  # cluster_processing_protocol_version

    return sock


def send_packet_expect_rejection(node, packet):
    """Send a raw packet to the ClickHouse native port and expect rejection.

    The server should close the connection or send an error response.
    Returns True if the connection was rejected (as expected).
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect((node.ip_address, 9000))
        sock.sendall(packet)
        # Try to receive — we expect the server to close the connection
        # or send an exception packet
        try:
            response = sock.recv(4096)
            # If we get a response, check it is an exception (packet type 2)
            if response and response[0] == 2:
                return True  # Exception packet — rejected as expected
            # Empty response means connection closed
            assert len(response) == 0
        except (ConnectionResetError, BrokenPipeError):
            return True  # Connection reset — rejected as expected
        except socket.timeout:
            return False  # Server did not reject in time
    finally:
        sock.close()


@pytest.mark.parametrize(
    "field_name",
    ["client_name", "default_db", "user", "password"],
)
def test_oversized_hello_field_rejected(started_cluster, field_name):
    """Each Hello string field exceeding 64 KB must be rejected."""
    packet = build_hello_packet(oversized_field=field_name)
    assert send_packet_expect_rejection(
        node, packet
    ), f"Server did not reject oversized {field_name}"


def test_oversized_cluster_name_rejected(started_cluster):
    """The cluster field in processClusterNameAndSalt must be rejected
    when exceeding 64 KB (reachable via interserver marker user)."""
    packet = build_interserver_hello_with_oversized_cluster()
    assert send_packet_expect_rejection(
        node, packet
    ), "Server did not reject oversized cluster name"


def test_boundary_just_over_limit_rejected(started_cluster):
    """A string of exactly 64 KB + 1 byte must be rejected."""
    packet = build_hello_packet(
        oversized_field="client_name",
        oversized_size=MAX_HELLO_STRING_SIZE + 1,
    )
    assert send_packet_expect_rejection(
        node, packet
    ), "Server did not reject string of 64 KB + 1"


def test_boundary_at_limit_accepted(started_cluster):
    """A string of exactly 64 KB must be accepted (not rejected)."""
    # Build a Hello packet where client_name is exactly 64 KB
    large_name = b"A" * MAX_HELLO_STRING_SIZE
    packet = build_hello_packet(client_name=large_name)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect((node.ip_address, 9000))
        sock.sendall(packet)
        # The server should NOT reject this — it should proceed to auth
        # (which will fail because user is "default" with no password,
        # but that is after reading all fields successfully).
        response = sock.recv(4096)
        # We expect either a Hello response (packet type 0) or an
        # Exception (packet type 2) from auth failure — NOT a connection
        # reset from TOO_LARGE_STRING_SIZE.
        assert len(response) > 0, "Server closed connection for a 64 KB string"
    finally:
        sock.close()


@pytest.mark.parametrize(
    "field_name",
    ["quota_key", "proto_send_chunked", "proto_recv_chunked"],
)
def test_oversized_addendum_field_rejected(started_cluster, field_name):
    """Addendum string fields exceeding 64 KB must be rejected.

    The addendum is sent after the Hello handshake completes.
    quota_key is sent when protocol >= 54458,
    proto_send_chunked/proto_recv_chunked when protocol >= 54470.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect((node.ip_address, 9000))
        complete_hello_handshake(sock, tcp_protocol_version=54471)

        # Build addendum with one oversized field
        parts = []

        # quota_key (protocol >= 54458)
        if field_name == "quota_key":
            parts.append(encode_oversized_string(OVERSIZED))
        else:
            parts.append(encode_string(b""))

        # proto_send_chunked_cl, proto_recv_chunked_cl (protocol >= 54470)
        if field_name == "proto_send_chunked":
            parts.append(encode_oversized_string(OVERSIZED))
        else:
            parts.append(encode_string(b"notchunked"))

        if field_name == "proto_recv_chunked":
            parts.append(encode_oversized_string(OVERSIZED))
        else:
            parts.append(encode_string(b"notchunked"))

        sock.sendall(b"".join(parts))
        response = b""

        # Expect rejection
        try:
            response = sock.recv(4096)
            if response and response[0] == 2:
                return  # Exception packet — rejected as expected
            assert len(response) == 0, f"Server did not reject oversized {field_name}, got response: {response}"
        except (ConnectionResetError, BrokenPipeError):
            pass  # Connection reset — rejected as expected
    finally:
        sock.close()


def test_oversized_unexpected_hello_rejected(started_cluster):
    """processUnexpectedHello string fields exceeding 64 KB must be rejected.

    After completing the handshake + addendum, sending a Hello packet (type 0)
    instead of a Query triggers processUnexpectedHello which reads 4 strings.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect((node.ip_address, 9000))
        complete_hello_handshake(sock, tcp_protocol_version=54471)

        # Send valid addendum to complete the handshake
        parts = []
        parts.append(encode_string(b""))  # quota_key
        parts.append(encode_string(b"notchunked"))  # proto_send_chunked
        parts.append(encode_string(b"notchunked"))  # proto_recv_chunked
        # parallel_replicas_protocol_version (protocol >= 54471)
        parts.append(encode_varuint(0))
        sock.sendall(b"".join(parts))

        # Now send a Hello packet (type 0) with an oversized string —
        # this triggers processUnexpectedHello
        unexpected = []
        unexpected.append(encode_varuint(0))  # Client::Hello = 0
        unexpected.append(encode_oversized_string(OVERSIZED))  # oversized client_name
        sock.sendall(b"".join(unexpected))

        # Expect rejection
        try:
            response = sock.recv(4096)
            if response and response[0] == 2:
                return  # Exception packet — rejected as expected
            assert len(response) == 0, "Server did not reject oversized unexpected Hello"
        except (ConnectionResetError, BrokenPipeError):
            pass  # Connection reset — rejected as expected
    finally:
        sock.close()


def test_slowloris_handshake_timeout(started_cluster):
    """A client that trickles data too slowly must be disconnected
    by the handshake wall-clock timeout (set to 3 seconds in test config)."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect((node.ip_address, 9000))
        # Send just the Hello packet type byte, then trickle slowly
        sock.sendall(encode_varuint(0))  # Client::Hello = 0
        # Send client_name length prefix claiming a small string (10 bytes)
        sock.sendall(encode_varuint(10))
        # Trickle 1 byte per second — the handshake timeout (3s) should fire
        # before we finish sending the 10-byte string
        for i in range(10):
            time.sleep(1)
            try:
                sock.sendall(b"A")
            except (BrokenPipeError, ConnectionResetError, OSError):
                return  # Server closed the connection — expected
        # If we sent all 10 bytes, try to send more / receive response
        # The server should have closed the connection by now
        try:
            response = sock.recv(4096)
            # Exception packet (type 2) with timeout error is acceptable
            if response and response[0] == 2:
                return
            assert len(response) == 0, "Server did not enforce handshake timeout"
        except (ConnectionResetError, BrokenPipeError, OSError):
            pass  # Server closed the connection — expected
    finally:
        sock.close()


def test_server_healthy_after_rejections(started_cluster):
    """After rejecting oversized packets, the server must still be healthy."""
    result = node.query("SELECT 1")
    assert result.strip() == "1"
