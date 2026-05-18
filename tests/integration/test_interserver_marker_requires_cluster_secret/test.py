"""Regression test for the interserver-marker authentication bypass.

Before the fix, a client could enter interserver mode by sending the
`USER_INTERSERVER_MARKER` user even when the named cluster was not
configured with a `<secret>`. Once in interserver mode, the client
could exercise pre-auth protocol packets (e.g. `TablesStatusRequest`)
that rely on a `fake_interserver_context` and learn information about
local tables without ever proving knowledge of the cluster secret.

The fix in `receiveHello` rejects the marker at the Hello phase when
the named cluster does not exist or has no `<secret>` configured.
"""

import socket
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/clusters.xml"],
)

USER_INTERSERVER_MARKER = " INTERSERVER SECRET "


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


def build_interserver_hello(cluster_name):
    """Build a native protocol Hello packet that enters the interserver
    path with the given cluster name (and a 32-byte salt)."""
    parts = []

    # Client::Hello = 0
    parts.append(encode_varuint(0))
    parts.append(encode_string(b"test"))  # client_name
    parts.append(encode_varuint(24))  # version_major
    parts.append(encode_varuint(1))  # version_minor
    parts.append(encode_varuint(54471))  # tcp_protocol_version
    parts.append(encode_string(b""))  # default_db
    parts.append(encode_string(USER_INTERSERVER_MARKER))  # user
    parts.append(encode_string(b""))  # password — empty for interserver

    # The server then calls `processClusterNameAndSalt` which reads
    # cluster and salt, both as length-prefixed strings (the salt is
    # capped at 32 bytes by the `max_string_size` argument).
    parts.append(encode_string(cluster_name))
    parts.append(encode_string(b"A" * 32))

    return b"".join(parts)


def send_hello_and_classify_response(node, packet):
    """Connect, send the packet, return ('exception', body) or
    ('closed', None) depending on how the server reacted.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    try:
        sock.connect((node.ip_address, 9000))
        sock.sendall(packet)
        try:
            response = sock.recv(65536)
        except (ConnectionResetError, BrokenPipeError):
            return ("closed", None)
        if not response:
            return ("closed", None)
        # Server::Exception = 2
        if response[0] == 2:
            return ("exception", response)
        return ("other", response)
    finally:
        sock.close()


def test_unknown_cluster_rejected(started_cluster):
    """Sending the interserver marker with a cluster that does not
    exist must be rejected (the connection is closed or an exception
    is returned)."""
    packet = build_interserver_hello(b"nonexistent_cluster_for_test")
    kind, _ = send_hello_and_classify_response(node, packet)
    assert kind in ("exception", "closed"), (
        f"Server did not reject interserver marker with unknown cluster, got: {kind}"
    )


def test_cluster_without_secret_rejected(started_cluster):
    """Sending the interserver marker with a cluster that exists but
    has no `<secret>` configured must also be rejected — this is the
    primary attack vector fixed by `receiveHello`."""
    packet = build_interserver_hello(b"no_secret_cluster")
    kind, _ = send_hello_and_classify_response(node, packet)
    assert kind in ("exception", "closed"), (
        f"Server did not reject interserver marker for secret-less cluster, got: {kind}"
    )
    # The server must log `AUTHENTICATION_FAILED` for the rejected
    # marker — confirm the rejection happened in `receiveHello`, not
    # because of some unrelated parsing error.
    assert node.contains_in_log(
        "Interserver authentication failed: cluster 'no_secret_cluster' is not configured with a secret"
    ), "Expected AUTHENTICATION_FAILED log message for cluster without secret"
