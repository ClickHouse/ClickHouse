import socket

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

# Regression tests for PR #99854: an interserver `TablesStatusRequest` must not return
# table existence / readonly / replication-delay status to a peer that has not proven
# knowledge of the cluster `<secret>`. Two rejection paths are covered:
#
#  * old protocol (no hash) + `interserver_tables_status_require_auth` -> rejected;
#  * new protocol with a wrong cluster secret -> hash validation fails -> rejected.
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


def test_old_protocol_unauthenticated_request_is_rejected(started_cluster):
    """An old-protocol peer sends no secret hash; with the require-auth setting on
    (the default), the server must reject it instead of disclosing table status."""
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
