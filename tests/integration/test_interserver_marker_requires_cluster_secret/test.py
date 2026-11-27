"""Regression test for the interserver-marker authentication bypass.

Before the fix, a client could enter interserver mode by sending the
`USER_INTERSERVER_MARKER` user even when the named cluster was not
configured with a `<secret>`. Once in interserver mode, the client
could exercise pre-auth protocol packets (e.g. `TablesStatusRequest`)
that rely on a `fake_interserver_context` and learn information about
local tables without ever proving knowledge of the cluster secret.

The fix in `receiveHello` rejects the marker at the Hello phase when
the named cluster does not exist or has no `<secret>` configured. The
rejection closes the connection without serializing the exception back
(an unauthenticated peer must not receive anything, not even whether
the named cluster exists), and is recorded in `system.session_log` as
a `LoginFailure` so the rejected probes stay visible to operators.
"""

import socket

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/config.d/clusters.xml",
        "configs/config.d/session_log.xml",
    ],
    user_configs=[
        "configs/users.d/profiles.xml",
    ],
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


def build_interserver_hello(cluster_name, tcp_protocol_version=54471):
    """Build a native protocol Hello packet that enters the interserver
    path with the given cluster name (and a 32-byte salt)."""
    parts = []

    # Client::Hello = 0
    parts.append(encode_varuint(0))
    parts.append(encode_string(b"test"))  # client_name
    parts.append(encode_varuint(24))  # version_major
    parts.append(encode_varuint(1))  # version_minor
    parts.append(encode_varuint(tcp_protocol_version))
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
    exist must be rejected by closing the connection without returning
    anything — an unauthenticated peer must not even learn whether the
    cluster exists."""
    packet = build_interserver_hello(b"nonexistent_cluster_for_test")
    kind, _ = send_hello_and_classify_response(node, packet)
    assert kind == "closed", (
        f"Server did not silently reject interserver marker with unknown cluster, got: {kind}"
    )


def test_cluster_without_secret_rejected(started_cluster):
    """Sending the interserver marker with a cluster that exists but
    has no `<secret>` configured must also be rejected — this is the
    primary attack vector fixed by `receiveHello`."""
    packet = build_interserver_hello(b"no_secret_cluster")
    kind, _ = send_hello_and_classify_response(node, packet)
    assert kind == "closed", (
        f"Server did not silently reject interserver marker for secret-less cluster, got: {kind}"
    )
    # The server must log `AUTHENTICATION_FAILED` for the rejected
    # marker — confirm the rejection happened in `receiveHello`, not
    # because of some unrelated parsing error.
    assert node.contains_in_log(
        "Interserver authentication failed: cluster 'no_secret_cluster' is not configured with a secret"
    ), "Expected AUTHENTICATION_FAILED log message for cluster without secret"
    # The rejection must stay visible to operators: it is routed through
    # `Session::onAuthenticationFailure`, which emits a `LoginFailure`
    # row into `system.session_log`.
    node.query("SYSTEM FLUSH LOGS session_log")
    assert (
        int(
            node.query(
                "SELECT count() FROM system.session_log "
                "WHERE type = 'LoginFailure' AND interface = 'TCP_Interserver' "
                "AND failure_reason LIKE '%is not configured with a secret%'"
            )
        )
        > 0
    ), "Expected a LoginFailure row in system.session_log for the rejected interserver marker"


def test_old_protocol_revision_rejected(started_cluster):
    """Interserver mode needs a revision that carries the server nonce (54462);
    an older peer is rejected in the handshake, before its cluster is looked up."""
    packet = build_interserver_hello(b"no_secret_cluster", tcp_protocol_version=54461)
    kind, _ = send_hello_and_classify_response(node, packet)
    assert kind == "closed", (
        f"Server did not silently reject interserver marker with an old revision, got: {kind}"
    )
    failure_reason = "Interserver mode is not supported for client protocol revision 54461, the minimum is 54462"
    assert node.contains_in_log(failure_reason)
    node.query("SYSTEM FLUSH LOGS session_log")
    assert (
        int(
            node.query(
                "SELECT count() FROM system.session_log "
                "WHERE type = 'LoginFailure' AND interface = 'TCP_Interserver' "
                f"AND failure_reason = '{failure_reason}'"
            )
        )
        > 0
    )


def send_forged_secondary_query(user, password, query, forged_initial_user):
    """Send one query over a plain (non-interserver) connection, forging query_kind=SECONDARY_QUERY
    and a client-chosen initial_user. Return the raw bytes the server sends back."""
    # A revision accepted by the server but below DBMS_MIN_PROTOCOL_VERSION_WITH_CHUNKED_PACKETS
    # (54470): new enough for the quota-key addendum field, old enough to avoid chunked framing and
    # its negotiation, so the Hello, addendum, Query and Data packets are plain back-to-back bytes.
    client_revision = 54462

    # A regular (non-interserver) Hello: authenticates as a normal user with a password.
    hello = b"".join(
        [
            encode_varuint(0),  # Client::Hello
            encode_string(b"test"),  # client_name
            encode_varuint(24),  # version_major
            encode_varuint(1),  # version_minor
            encode_varuint(client_revision),
            encode_string(b""),  # default database
            encode_string(user),
            encode_string(password),
        ]
    )
    addendum = encode_string(b"")  # quota key

    # A Query packet that claims to be a SECONDARY_QUERY on behalf of another initial_user.
    client_info = b"".join(
        [
            b"\x02",  # query_kind = SECONDARY_QUERY
            encode_string(forged_initial_user),  # forged initial_user
            encode_string(b""),  # initial_query_id
            encode_string(b"127.0.0.1:9000"),  # initial_address
            (0).to_bytes(8, "little"),  # initial_query_start_time_microseconds
            b"\x01",  # interface = TCP
            encode_string(b""),  # os_user
            encode_string(b""),  # client_hostname
            encode_string(b"test"),  # client_name
            encode_varuint(24),  # version_major
            encode_varuint(1),  # version_minor
            encode_varuint(client_revision),  # client_tcp_protocol_version
            encode_string(b""),  # quota_key
            encode_varuint(0),  # distributed_depth
            encode_varuint(0),  # version_patch
            b"\x00",  # no OpenTelemetry trace context
            encode_varuint(0),  # collaborate_with_initiator
            encode_varuint(0),  # obsolete_count_participating_replicas
            encode_varuint(0),  # number_of_current_replica
        ]
    )
    query_packet = b"".join(
        [
            encode_varuint(1),  # Client::Query
            encode_string(b""),  # query_id
            client_info,
            encode_string(b""),  # end of settings
            encode_string(b"\x00" * 32),  # interserver hash (ignored without the secret handshake)
            encode_varuint(2),  # stage = Complete
            encode_varuint(0),  # compression disabled
            encode_string(query),
            encode_string(b""),  # end of parameters
        ]
    )

    # An empty Data block signals end of input, so the server runs the query to completion.
    data_block = b"".join(
        [
            encode_varuint(2),  # Client::Data
            encode_string(b""),  # temporary table name
            encode_varuint(1),  # BlockInfo field: is_overflows
            b"\x00",  # is_overflows = false
            encode_varuint(2),  # BlockInfo field: bucket_num
            (-1).to_bytes(4, "little", signed=True),  # bucket_num = -1
            encode_varuint(0),  # end of BlockInfo
            encode_varuint(0),  # columns
            encode_varuint(0),  # rows
        ]
    )

    packet = hello + addendum + query_packet + data_block
    response = b""
    with socket.create_connection((node.ip_address, 9000), timeout=20) as connection:
        connection.settimeout(5)
        connection.sendall(packet)
        # The server keeps the connection open after the result, so read until it goes idle.
        try:
            while True:
                chunk = connection.recv(65536)
                if not chunk:
                    break
                response += chunk
        except (socket.timeout, ConnectionResetError, BrokenPipeError):
            pass
    return response


def test_forged_secondary_query_cannot_impersonate_on_secret_cluster(started_cluster):
    """A user forges a SECONDARY_QUERY and reads a Distributed table over a secret cluster. The node
    ignores a client-chosen initial_user of a user without the IMPERSONATE privilege for it, so the
    query reaches the shard as the attacker and never gets the admin-only data -- neither by
    impersonating 'admin' nor by sending an empty initial_user (which the shard would otherwise
    authenticate as no user, i.e. with full access)."""
    try:
        # A table only `admin` may read, exposed through a Distributed table over the secret cluster.
        node.query(
            "CREATE TABLE default.secret_data (secret String) ENGINE = MergeTree ORDER BY secret"
        )
        node.query("INSERT INTO default.secret_data VALUES ('TOP_SECRET_ADMIN_ONLY')")
        node.query(
            "CREATE TABLE default.secret_dist AS default.secret_data "
            "ENGINE = Distributed(secret_cluster, default, secret_data)"
        )
        node.query("CREATE USER admin IDENTIFIED WITH no_password HOST ANY")
        node.query("GRANT SELECT ON default.secret_data TO admin")
        # The attacker may read only the Distributed table, never the secret table directly.
        node.query("CREATE USER attacker IDENTIFIED WITH no_password HOST ANY")
        node.query("GRANT SELECT ON default.secret_dist TO attacker")

        # The forged initial_user is ignored whether it impersonates 'admin' or is empty.
        for forged_initial_user in [b"admin", b""]:
            response = send_forged_secondary_query(
                user=b"attacker",
                password=b"",
                query="SELECT secret FROM default.secret_dist",
                forged_initial_user=forged_initial_user,
            )

            # The attacker must not obtain the admin-only data.
            assert b"TOP_SECRET_ADMIN_ONLY" not in response
            # The shard authenticates the attacker, who has no access to the table behind it.
            assert b"attacker: Not enough privileges" in response
    finally:
        node.query("DROP TABLE IF EXISTS default.secret_dist SYNC")
        node.query("DROP TABLE IF EXISTS default.secret_data SYNC")
        node.query("DROP USER IF EXISTS attacker, admin")


def test_mixed_secret_and_insecure_cluster_rejected(started_cluster):
    """A non-secret Distributed table whose shard re-forwards over a secret one leaves the shard with
    initial_user != current_user, so the fix rejects it instead of laundering an unverified
    initiator (the non-secret hop cannot vouch for it)."""
    try:
        node.query(
            "CREATE TABLE default.mixed_data (secret String) ENGINE = MergeTree ORDER BY secret"
        )
        node.query("INSERT INTO default.mixed_data VALUES ('TOP_SECRET_ADMIN_ONLY')")
        # A non-secret Distributed table (outer) whose shard reads a secret Distributed table (inner).
        node.query(
            "CREATE TABLE default.mixed_inner_dist AS default.mixed_data "
            "ENGINE = Distributed(secret_cluster, default, mixed_data)"
        )
        node.query(
            "CREATE TABLE default.mixed_outer_dist AS default.mixed_data "
            "ENGINE = Distributed(insecure_cluster, default, mixed_inner_dist)"
        )
        # A non-default user, so the shard sees initial_user (mixed_user) != current_user (default).
        node.query("CREATE USER mixed_user IDENTIFIED WITH no_password HOST ANY")
        node.query("GRANT SELECT ON default.* TO mixed_user")

        error = node.query_and_get_error(
            "SELECT secret FROM default.mixed_outer_dist", user="mixed_user"
        )
        assert "Refusing to forward a query over the cluster secret" in error
    finally:
        node.query("DROP TABLE IF EXISTS default.mixed_outer_dist SYNC")
        node.query("DROP TABLE IF EXISTS default.mixed_inner_dist SYNC")
        node.query("DROP TABLE IF EXISTS default.mixed_data SYNC")
        node.query("DROP USER IF EXISTS mixed_user")
