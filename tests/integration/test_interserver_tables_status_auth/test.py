import socket
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

# Regression tests for pre-authentication interserver packet handling: in interserver mode the
# connection is not authenticated until the `Query` packet is processed, so a packet that the
# server rejects before then must disclose nothing, and a rejected `Data` packet must not have
# its payload deserialized. Three rejection paths are covered:
#
#  * `TablesStatusRequest`, old protocol (no hash) + `interserver_tables_status_require_auth`;
#  * `TablesStatusRequest`, new protocol signed with the wrong cluster secret;
#  * `Data` before any `Query` -> rejected without the Native block being read.
#
# The legitimate authenticated path is covered by `test_distributed_inter_server_secret`.

cluster = ClickHouseCluster(__file__)
node_a = cluster.add_instance("node_a", main_configs=["configs/secret_a.xml"])
node_b = cluster.add_instance("node_b", main_configs=["configs/secret_b.xml"])

# Old revision: below DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_TABLES_STATUS (no hash),
# below DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS (simple framing) and below
# DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_V2 (no nonce in the server Hello).
OLD_REVISION = 54449
USER_INTERSERVER_MARKER = " INTERSERVER SECRET "

# A type name no other test can produce, so the log assertions below cannot be crossed.
BOGUS_TYPE = "NoSuchTypeGroeneAI"
BOGUS_TYPE_READ = f"Unknown data type family: {BOGUS_TYPE}"
# An interserver connection is unauthenticated until its `Query` packet, so a `Data` packet arriving
# before then is reported as an authentication failure (an ordinary client gets
# `UNEXPECTED_PACKET_FROM_CLIENT`); matching that wording also proves interserver mode was reached.
DATA_REJECTED = "Unexpected data packet received before interserver authentication"


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


def open_interserver_connection(node):
    """Connect and complete an interserver handshake that the server accepts. The Hello
    names cluster `mismatch` (which has a secret) so the handshake is accepted into
    interserver mode, where nothing is authenticated yet."""
    hello = (
        varuint(0)
        + varstring("test")           # client name
        + varuint(24)                 # version major
        + varuint(3)                  # version minor
        + varuint(OLD_REVISION)       # tcp protocol revision
        + varstring("")               # default database
        + varstring(USER_INTERSERVER_MARKER)
        + varstring("")               # password (empty -> interserver mode)
        + varstring("mismatch")       # cluster name (must exist and have a secret)
        + varstring("")               # salt
    )
    sock = socket.create_connection((node.ip_address, 9000), timeout=20)
    sock.settimeout(20)
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
    return sock


def wait_for_log_growth(node, needles, baseline, timeout=60):
    """Poll until one of `needles` occurs more often in the node's log than in `baseline`,
    returning the final counts (parallel to `needles`) grown or not. The verdict is logged
    while the connection handler unwinds, so an assertion evaluated without this barrier
    could be satisfied by reading the log too early."""
    deadline = time.monotonic() + timeout
    while True:
        counts = [int(node.count_in_log(needle)) for needle in needles]
        if any(c > b for c, b in zip(counts, baseline)) or time.monotonic() > deadline:
            return counts
        time.sleep(0.5)


def test_old_protocol_unauthenticated_request_is_rejected(started_cluster):
    """An old-protocol peer sends no secret hash; with the require-auth setting on
    (the default), the server must reject it instead of disclosing table status.
    The Hello names the existing cluster `mismatch` (which has a secret) so that the
    handshake itself is accepted and the rejection is exercised at the
    `TablesStatusRequest` stage; a Hello with an unknown or secret-less cluster is
    already rejected during the handshake (covered by
    `test_interserver_marker_requires_cluster_secret`)."""
    hello = (
        varuint(0)
        + varstring("test")           # client name
        + varuint(24)                 # version major
        + varuint(3)                  # version minor
        + varuint(OLD_REVISION)       # tcp protocol revision
        + varstring("")               # default database
        + varstring(USER_INTERSERVER_MARKER)
        + varstring("")               # password (empty -> interserver mode)
        + varstring("mismatch")       # cluster name (must exist and have a secret, or the Hello itself is rejected)
        + varstring("")               # salt
    )
    tsr = varuint(5) + varuint(1) + varstring("default") + varstring("any_table")

    sock = socket.create_connection((node_a.ip_address, 9000), timeout=20)
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


def test_new_protocol_wrong_secret_request_is_rejected(started_cluster):
    """A new-protocol peer that signs the request with the wrong cluster secret must be
    rejected when the server validates the hash. node_a and node_b configure the same
    cluster `mismatch` with different secrets, so node_a's hash fails to validate on
    node_b during the `TablesStatusRequest` issued at connection establishment."""
    node_b.query("DROP TABLE IF EXISTS t_local SYNC")
    node_a.query("DROP TABLE IF EXISTS t_dist SYNC")
    node_b.query("CREATE TABLE t_local (x UInt32) ENGINE = MergeTree ORDER BY x")
    node_a.query(
        "CREATE TABLE t_dist (x UInt32) "
        "ENGINE = Distributed(mismatch, default, t_local, rand())"
    )

    # The query reaches out to node_b; with prefer_localhost_replica=0 and the staleness
    # check enabled, connection establishment sends a TablesStatusRequest first.
    with pytest.raises(QueryRuntimeException):
        node_a.query(
            "SELECT count() FROM t_dist SETTINGS "
            "prefer_localhost_replica=0, max_replica_delay_for_distributed_queries=300, "
            "fallback_to_stale_replicas_for_distributed_queries=1"
        )

    # The rejection must come from the TablesStatusRequest hash check on node_b — proving
    # the new-revision path ran (a pre-fix binary would have no such log line).
    assert node_b.contains_in_log(
        "Interserver authentication failed for TablesStatusRequest"
    ), "node_b did not reject the wrong-secret TablesStatusRequest via hash validation"


def test_data_packet_before_query_is_not_deserialized(started_cluster):
    """A `Data` packet arriving before any `Query` must be rejected without its payload
    being read. Two connections cover the two halves of that: the first sends a complete
    block declaring a column type that does not exist, so reading the payload would hand
    that name to `DataTypeFactory` and log it as an unknown family; the second sends the
    packet type alone and half-closes, so a handler that needs any payload byte reaches
    end-of-stream instead of the rejection."""
    # Uncompressed: the compression method is only negotiated while a query is processed.
    # rows=0 carries no column data, since the type name precedes it on the wire.
    data_packet = (
        varuint(2)                    # Protocol::Client::Data
        + varstring("")               # external table name
        + varuint(0)                  # BlockInfo field terminator
        + varuint(1)                  # columns
        + varuint(0)                  # rows
        + varstring("c")              # column name
        + varstring(BOGUS_TYPE)       # column type name
    )

    before_read = int(node_a.count_in_log(BOGUS_TYPE_READ))
    before_rejected = int(node_a.count_in_log(DATA_REJECTED))

    sock = open_interserver_connection(node_a)
    try:
        sock.sendall(data_packet)
        try:
            data = sock.recv(4096)
        except ConnectionResetError:
            data = b""
        assert not data, "server answered a Data packet sent before any query"
    finally:
        sock.close()

    after_read, after_rejected = wait_for_log_growth(
        node_a, [BOGUS_TYPE_READ, DATA_REJECTED], [before_read, before_rejected]
    )

    assert after_read == before_read, (
        f"the type name {BOGUS_TYPE} came off the wire and reached DataTypeFactory: the "
        "Native block was deserialized without the cluster secret being proved"
    )
    assert (
        after_rejected > before_rejected
    ), "the Data packet was not rejected before interserver authentication"

    # The block above shows no type was constructed; this one shows no payload byte was
    # needed at all. The write side is closed right after the packet type, so a handler
    # that reads the external table name first ends at end-of-stream and never rejects.
    before_type_only = int(node_a.count_in_log(DATA_REJECTED))

    sock = open_interserver_connection(node_a)
    try:
        sock.sendall(varuint(2))    # Protocol::Client::Data, with no body at all
        sock.shutdown(socket.SHUT_WR)
        try:
            data = sock.recv(4096)
        except ConnectionResetError:
            data = b""
        assert not data, "server answered a Data packet sent before any query"
    finally:
        sock.close()

    (after_type_only,) = wait_for_log_growth(
        node_a, [DATA_REJECTED], [before_type_only]
    )

    assert after_type_only > before_type_only, (
        "the Data packet was not rejected on its packet type alone, so a payload byte "
        "was required"
    )
