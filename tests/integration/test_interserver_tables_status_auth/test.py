import socket

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

# Regression tests for PR #99854: an interserver `TablesStatusRequest` must not return
# table existence / readonly / replication-delay status to a peer that has not proven
# knowledge of the cluster `<secret>`. Four paths are covered:
#
#  * old protocol (no hash), default settings -> placeholder response that does not depend
#    on the state of the tables, so an old initiator keeps working without disclosure;
#  * old protocol (no hash) + `interserver_tables_status_require_auth` -> rejected;
#  * new protocol with a wrong cluster secret -> hash validation fails -> rejected;
#  * a real mixed-version cluster (26.6 initiator, current build serving the data) ->
#    the `Distributed` query works, which is the rolling upgrade PR #113602 restored.
#
# The legitimate authenticated path is covered by `test_distributed_inter_server_secret`.

cluster = ClickHouseCluster(__file__)
node_a = cluster.add_instance("node_a", main_configs=["configs/secret_a.xml"])
node_b = cluster.add_instance("node_b", main_configs=["configs/secret_b.xml"])
node_default = cluster.add_instance(
    "node_default", main_configs=["configs/secret_default.xml"]
)

# 26.6 is the last release before the interserver `TablesStatusRequest` hash: its protocol
# revision is 54485, below DBMS_MIN_REVISION_WITH_INTERSERVER_SECRET_TABLES_STATUS (54487),
# so its `ConnectionEstablisher` sends the request unsigned - the exact rolling-upgrade
# skew that was reported as broken.
node_old = cluster.add_instance(
    "node_old",
    main_configs=["configs/secret_upgrade.xml"],
    image="clickhouse/clickhouse-server",
    tag="26.6",
    stay_alive=True,
    with_installed_binary=True,
)
node_new = cluster.add_instance("node_new", main_configs=["configs/secret_upgrade.xml"])

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


def connect_as_ordinary_client(node):
    """Handshake as a normal (non-interserver) client and return the socket."""
    hello = (
        varuint(0)
        + varstring("test")           # client name
        + varuint(24)                 # version major
        + varuint(3)                  # version minor
        + varuint(OLD_REVISION)       # tcp protocol revision
        + varstring("default")        # default database
        + varstring("default")        # user
        + varstring("")               # password
    )

    sock = socket.create_connection((node.ip_address, 9000), timeout=20)
    sock.settimeout(20)
    sock.sendall(hello)
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


def test_rolling_upgrade_initiator_on_old_version_can_query(started_cluster):
    """The reported regression, end to end: a 26.6 initiator (protocol revision 54485, so its
    `TablesStatusRequest` carries no cluster-secret hash) reading a `Distributed` table whose
    shard lives on the current build. The upgraded node must answer the unsigned request
    instead of rejecting it, so that `ConnectionEstablisher` accepts the replica and the
    initiator goes on to the `Query`, which does authenticate with the cluster secret. On the
    unfixed binary the request is rejected, the replica counts as a failed connection, and
    this query fails."""
    # Guard against the old image silently not being used: on the current build both sides
    # would negotiate 54487 and sign the request, and the test would pass without covering
    # the skew it exists for.
    old_version = node_old.query("SELECT version()").strip()
    assert tuple(int(part) for part in old_version.split(".")[:2]) < (26, 7), (
        f"node_old must run a release older than 26.7, got {old_version}"
    )

    node_new.query("DROP TABLE IF EXISTS t_local SYNC")
    node_new.query("CREATE TABLE t_local (x UInt32) ENGINE = MergeTree ORDER BY x")
    node_new.query("INSERT INTO t_local SELECT number FROM numbers(7)")
    node_old.query("DROP TABLE IF EXISTS t_dist SYNC")
    node_old.query(
        "CREATE TABLE t_dist (x UInt32) "
        "ENGINE = Distributed(upgrade, default, t_local, rand())"
    )

    # prefer_localhost_replica=0 and the staleness check are what make connection
    # establishment send a TablesStatusRequest before the query.
    result = node_old.query(
        "SELECT count() FROM t_dist SETTINGS "
        "prefer_localhost_replica=0, max_replica_delay_for_distributed_queries=300, "
        "fallback_to_stale_replicas_for_distributed_queries=1"
    )
    assert result.strip() == "7", (
        "a Distributed query initiated on a not-yet-upgraded node did not return the rows of "
        f"the upgraded node: {result!r}"
    )


def test_rolling_upgrade_initiator_on_old_version_is_rejected_in_strict_mode(
    started_cluster,
):
    """The same 26.6 initiator, but the node serving the shard enables
    `interserver_tables_status_require_auth` - which is what the server did by default before
    this change. Its query must fail, which is what makes the test above load-bearing: it is
    the placeholder response, not anything else in the setup, that keeps the rolling upgrade
    working."""
    node_a.query("DROP TABLE IF EXISTS t_local SYNC")
    node_a.query("CREATE TABLE t_local (x UInt32) ENGINE = MergeTree ORDER BY x")
    node_old.query("DROP TABLE IF EXISTS t_dist_strict SYNC")
    node_old.query(
        "CREATE TABLE t_dist_strict (x UInt32) "
        "ENGINE = Distributed(upgrade_strict, default, t_local, rand())"
    )

    with pytest.raises(QueryRuntimeException):
        node_old.query(
            "SELECT count() FROM t_dist_strict SETTINGS "
            "prefer_localhost_replica=0, max_replica_delay_for_distributed_queries=300, "
            "fallback_to_stale_replicas_for_distributed_queries=1"
        )


def test_interserver_request_table_count_is_bounded(started_cluster):
    """The request body is deserialized before the peer is authenticated, so the number of
    tables an interserver peer can ask about is capped at
    `MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST`. At the cap the request is still answered;
    above it the connection is closed without a response (any exception on an
    unauthenticated interserver connection closes it silently)."""
    sock = connect_as_old_interserver_peer(node_default)
    try:
        sock.sendall(
            tables_status_request([("default", f"t{i}") for i in range(1024)])
        )
        states = read_tables_status_response(sock)
    finally:
        sock.close()
    assert len(states) == 1024, f"expected 1024 table states, got {len(states)}"

    sock = connect_as_old_interserver_peer(node_default)
    try:
        sock.sendall(
            tables_status_request([("default", f"t{i}") for i in range(1025)])
        )
        try:
            data = sock.recv(4096)
        except ConnectionResetError:
            data = b""
        assert not data, (
            "server answered an interserver TablesStatusRequest that exceeds the table-count "
            "bound"
        )
    finally:
        sock.close()


def test_interserver_request_name_length_is_bounded(started_cluster):
    """The table count alone does not bound the request: `readStringBinary` allocates the
    declared size of a name before reading its bytes, so a single name declared as 1 GiB
    would be an unauthenticated allocation. Names are capped as well, and the request is
    refused before the declared bytes are allocated."""
    sock = connect_as_old_interserver_peer(node_default)
    try:
        # A name whose declared length exceeds the cap. Only the length is sent - the point is
        # that the server must not allocate it while waiting for bytes that never arrive.
        oversized = varuint(5) + varuint(1) + varstring("default") + varuint(1 << 30)
        sock.sendall(oversized)
        try:
            data = sock.recv(4096)
        except ConnectionResetError:
            data = b""
        assert not data, (
            "server answered an interserver TablesStatusRequest declaring an oversized table name"
        )
    finally:
        sock.close()


def test_ordinary_client_table_count_is_not_bounded_by_the_interserver_limit(
    started_cluster,
):
    """The bound applies to the interserver path only: an ordinary authenticated client can
    still ask about more tables than `MAX_TABLES_IN_INTERSERVER_STATUS_REQUEST`, as it could
    before. None of the tables exist, so the response is empty - the point is that it is a
    response and not a `TOO_LARGE_ARRAY_SIZE` error."""
    sock = connect_as_ordinary_client(node_default)
    try:
        sock.sendall(
            tables_status_request([("default", f"t{i}") for i in range(1025)])
        )
        states = read_tables_status_response(sock)
    finally:
        sock.close()
    assert states == {}, f"unexpected table states for tables that do not exist: {states}"


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
