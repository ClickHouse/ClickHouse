import socket

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

# Regression tests for PR #99854: an interserver `TablesStatusRequest` must not return
# table existence / readonly / replication-delay status to a peer that has not proven
# knowledge of the cluster `<secret>`. Three paths are covered:
#
#  * old protocol (no hash), default settings -> placeholder response that does not depend
#    on the state of the tables, so an old initiator keeps working without disclosure;
#  * old protocol (no hash) + `interserver_tables_status_require_auth` -> rejected;
#  * new protocol with a wrong cluster secret -> hash validation fails -> rejected.
#
# The legitimate authenticated path is covered by `test_distributed_inter_server_secret`.

cluster = ClickHouseCluster(__file__)
node_a = cluster.add_instance("node_a", main_configs=["configs/secret_a.xml"])
node_b = cluster.add_instance("node_b", main_configs=["configs/secret_b.xml"])
node_default = cluster.add_instance(
    "node_default", main_configs=["configs/secret_default.xml"]
)

# Old revision: below DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_TABLES_STATUS (no hash),
# below DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS (simple framing) and below
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


def connect_as_old_interserver_peer(node):
    """Handshake as an old-protocol interserver peer and return the socket.

    The Hello names the existing cluster `mismatch` (which has a secret) so that the
    handshake itself is accepted and the behaviour under test is exercised at the
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


def tables_status_request(tables):
    """A `TablesStatusRequest` (client packet 5) without an authentication hash, as an
    older peer sends it."""
    body = varuint(len(tables))
    for database, table in tables:
        body += varstring(database) + varstring(table)
    return varuint(5) + body


def read_tables_status_response(sock):
    """Read a `TablesStatusResponse` (server packet 9) into {(database, table): status}."""
    packet_type = read_varuint(sock)
    assert packet_type == 9, f"expected TablesStatusResponse (9), got packet {packet_type}"
    states = {}
    for _ in range(read_varuint(sock)):
        database = read_varstring(sock).decode()
        table = read_varstring(sock).decode()
        is_replicated = recv_exact(sock, 1)[0]
        status = {"is_replicated": is_replicated, "absolute_delay": 0, "is_readonly": 0}
        if is_replicated:
            status["absolute_delay"] = read_varuint(sock)
            # `is_readonly` is written only from TABLE_READ_ONLY_CHECK (v54467) on, and
            # OLD_REVISION is below it.
        states[(database, table)] = status
    return states


def test_old_protocol_unauthenticated_request_gets_placeholder_response(started_cluster):
    """By default an old-protocol peer (which sends no secret hash) is answered with a
    placeholder response instead of an error, so a `Distributed` query initiated on a
    not-yet-upgraded node keeps working during a rolling upgrade. The response must not
    depend on the state of the tables: a table that does not exist is reported exactly
    like one that does, which is what makes it disclose nothing."""
    node_default.query("DROP TABLE IF EXISTS t_present SYNC")
    node_default.query("CREATE TABLE t_present (x UInt32) ENGINE = MergeTree ORDER BY x")

    sock = connect_as_old_interserver_peer(node_default)
    try:
        sock.sendall(
            tables_status_request([("default", "t_present"), ("default", "t_absent")])
        )
        states = read_tables_status_response(sock)
    finally:
        sock.close()

    assert states == {
        ("default", "t_present"): {
            "is_replicated": 0,
            "absolute_delay": 0,
            "is_readonly": 0,
        },
        ("default", "t_absent"): {
            "is_replicated": 0,
            "absolute_delay": 0,
            "is_readonly": 0,
        },
    }, (
        "the response to an unauthenticated interserver TablesStatusRequest depends on the "
        f"actual state of the tables (table status disclosed): {states}"
    )

    assert node_default.contains_in_log(
        "Answering an unauthenticated interserver TablesStatusRequest with a placeholder response"
    ), "the placeholder response was not logged, so a different path answered the request"


def test_old_protocol_unauthenticated_request_is_rejected(started_cluster):
    """With `interserver_tables_status_require_auth` enabled, an old-protocol peer that
    sends no secret hash must be rejected instead of getting the placeholder response."""
    sock = connect_as_old_interserver_peer(node_a)
    try:
        sock.sendall(tables_status_request([("default", "any_table")]))
        try:
            data = sock.recv(4096)
        except ConnectionResetError:
            data = b""
        assert not data, (
            "server returned data to an unauthenticated interserver TablesStatusRequest "
            "although interserver_tables_status_require_auth is enabled"
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
