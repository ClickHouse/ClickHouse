import socket

import pytest

from helpers.cluster import ClickHouseCluster

# Regression test for PR #99854: an interserver `TablesStatusRequest` must not return
# table existence / readonly / replication-delay status to a peer that has not proven
# knowledge of the cluster `<secret>`.
#
# The node sets `interserver_tables_status_require_auth = 1`, so a peer that connects in
# interserver mode with an old protocol revision (one that sends no secret hash) is
# rejected. Against a server build that predates the fix the setting is unknown and the
# request is served, so this test fails there (which is exactly what `Bugfix validation`
# checks). The legitimate authenticated path is covered by
# `test_distributed_inter_server_secret`.

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/require_auth.xml"])

# Old revision: below DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_TABLES_STATUS (no hash
# sent), below DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS (simple framing) and below
# DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 (no nonce in the server Hello).
OLD_REVISION = 54449
USER_INTERSERVER_MARKER = " INTERSERVER SECRET "


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def varuint(n):
    buf = bytearray()
    while n >= 0x80:
        buf.append((n & 0x7F) | 0x80)
        n >>= 7
    buf.append(n & 0x7F)
    return bytes(buf)


def varstring(s):
    b = s.encode() if isinstance(s, str) else bytes(s)
    return varuint(len(b)) + b


def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError()
        buf.extend(chunk)
    return buf


def read_varuint(sock):
    x = 0
    for i in range(9):
        b = recv_exact(sock, 1)[0]
        x |= (b & 0x7F) << (7 * i)
        if not (b & 0x80):
            return x
    return x


def read_varstring(sock):
    return recv_exact(sock, read_varuint(sock))


def test_unauthenticated_interserver_tables_status_is_rejected(started_cluster):
    # Interserver Hello (packet type 0), then the cluster name and salt that the server
    # reads in interserver mode. No secret hash is sent.
    hello = (
        varuint(0)
        + varstring("test")           # client name
        + varuint(24)                 # version major
        + varuint(3)                  # version minor
        + varuint(OLD_REVISION)       # tcp protocol revision
        + varstring("")               # default database
        + varstring(USER_INTERSERVER_MARKER)
        + varstring("")               # password (empty -> interserver mode)
        + varstring("")               # cluster name
        + varstring("")               # salt
    )

    # TablesStatusRequest (packet type 5): ask about a single table.
    tsr = varuint(5) + varuint(1) + varstring("default") + varstring("any_table")

    sock = socket.create_connection((node.ip_address, 9000), timeout=20)
    sock.settimeout(20)
    try:
        sock.sendall(hello)

        # Consume the server Hello (old-revision layout: no nonce, no chunking).
        read_varuint(sock)      # packet type (Hello)
        read_varstring(sock)    # server name
        read_varuint(sock)      # version major
        read_varuint(sock)      # version minor
        read_varuint(sock)      # revision
        read_varstring(sock)    # timezone
        read_varstring(sock)    # display name
        read_varuint(sock)      # version patch

        sock.sendall(tsr)

        # With the fix the server closes the connection without answering (empty recv or
        # an abrupt reset). Without the fix it replies with a TablesStatusResponse, i.e.
        # returns data -> this assertion fails.
        try:
            data = sock.recv(4096)
        except ConnectionResetError:
            data = b""
        assert not data, (
            "server returned data to an unauthenticated interserver TablesStatusRequest "
            "(table status disclosed without cluster-secret authentication)"
        )
    finally:
        sock.close()
