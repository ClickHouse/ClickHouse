import socket
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

# Regression tests for pre-authentication interserver packet handling: in interserver mode the
# connection is not authenticated until the `Query` packet is processed, so a packet that the
# server rejects before then must disclose nothing, and a rejected `Data` packet must not have
# its payload deserialized. An unsigned `TablesStatusRequest` is the one packet that is not
# rejected: it is answered with a response that hides the table status, i.e. does not depend on
# the state of the tables, so a rolling upgrade keeps working without anything being disclosed.
# Five paths are covered:
#
#  * `TablesStatusRequest`, old protocol (no hash), default settings -> table status hidden;
#  * `TablesStatusRequest`, old protocol (no hash) + `interserver_tables_status_require_auth`
#    -> rejected;
#  * `TablesStatusRequest`, new protocol signed with the wrong cluster secret -> rejected;
#  * `Data` before any `Query` -> rejected without the Native block being read;
#  * a real mixed-version cluster (26.6 initiator, current build serving the data) -> the
#    `Distributed` query works, which is what this change restored.
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


def test_old_protocol_unauthenticated_request_gets_table_status_hidden(started_cluster):
    """By default an old-protocol peer (which sends no secret hash) is answered with a
    response that hides the table status instead of an error, so a `Distributed` query initiated on a
    not-yet-upgraded node keeps working during a rolling upgrade. The response must not
    depend on the state of the tables: a table that does not exist is reported exactly
    like one that does, which is what makes it disclose nothing."""
    node_default.query("DROP TABLE IF EXISTS t_present SYNC")
    node_default.query("CREATE TABLE t_present (x UInt32) ENGINE = MergeTree ORDER BY x")

    sock = open_interserver_connection(node_default)
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
        "Answering an unauthenticated interserver TablesStatusRequest without the table status"
    ), "the status-hiding response was not logged, so a different path answered the request"


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
    hiding the table status, not anything else in the setup, that keeps the rolling upgrade
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


def test_old_protocol_unauthenticated_request_is_rejected(started_cluster):
    """With `interserver_tables_status_require_auth` enabled, an old-protocol peer that
    sends no secret hash must be rejected instead of getting a response that hides the table status."""
    sock = open_interserver_connection(node_a)
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
