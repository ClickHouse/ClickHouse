#!/usr/bin/env bash
# Tags: zookeeper
# An interserver `TablesStatusRequest` arrives before any `Query` has authenticated the connection
# with the cluster secret, so it must be answered without disclosing the table status: every
# requested table is reported present, not replicated and writable, whatever its actual state.
# An ordinary authenticated client still gets the real status.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_present (x UInt32) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_present', 'r1') ORDER BY x;
"

python3 - <<'PYEOF'
import os
import socket

HOST = os.environ["CLICKHOUSE_HOST"]
PORT = int(os.environ["CLICKHOUSE_PORT_TCP"])
DATABASE = os.environ["CLICKHOUSE_DATABASE"]
USER = os.environ.get("CLICKHOUSE_USER", "default")

# Below the addendum, nonce, read-only-check and chunked-packets revisions, so the handshake is a
# bare Hello exchange, no packet is framed and a table status has no `is_readonly`.
REVISION = 54449
USER_INTERSERVER_MARKER = " INTERSERVER SECRET "


def varuint(n):
    buf = bytearray()
    while n >= 0x80:
        buf.append((n & 0x7F) | 0x80)
        n >>= 7
    buf.append(n)
    return bytes(buf)


def varstring(s):
    b = s.encode()
    return varuint(len(b)) + b


def recv_exact(sock, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError()
        buf.extend(chunk)
    return bytes(buf)


def read_varuint(sock):
    x = 0
    for i in range(9):
        b = recv_exact(sock, 1)[0]
        x |= (b & 0x7F) << (7 * i)
        if not b & 0x80:
            break
    return x


def read_varstring(sock):
    return recv_exact(sock, read_varuint(sock)).decode()


def tables_status(user, password, cluster_and_salt):
    hello = (
        varuint(0)
        + varstring("test")
        + varuint(26)
        + varuint(3)
        + varuint(REVISION)
        + varstring(DATABASE)
        + varstring(user)
        + varstring(password)
        + cluster_and_salt
    )
    request = varuint(5) + varuint(2)
    for table in ("t_present", "t_absent"):
        request += varstring(DATABASE) + varstring(table)

    with socket.create_connection((HOST, PORT), timeout=30) as sock:
        sock.sendall(hello)
        packet = read_varuint(sock)
        assert packet == 0, f"expected server Hello, got packet {packet}"
        read_varstring(sock)  # server name
        read_varuint(sock)  # version major
        read_varuint(sock)  # version minor
        read_varuint(sock)  # revision
        read_varstring(sock)  # timezone
        read_varstring(sock)  # display name
        read_varuint(sock)  # version patch

        sock.sendall(request)
        packet = read_varuint(sock)
        assert packet == 9, f"expected TablesStatusResponse, got packet {packet}"
        states = []
        for _ in range(read_varuint(sock)):
            read_varstring(sock)  # database
            table = read_varstring(sock)
            is_replicated = recv_exact(sock, 1)[0]
            if is_replicated:
                read_varuint(sock)  # absolute_delay, timing-dependent
            states.append((table, is_replicated))
        return sorted(states)


def show(name, states):
    print(name)
    for state in states:
        print("\t".join(map(str, state)))


INTERSERVER = varstring("test_cluster_interserver_secret") + varstring("salt")

show("authenticated", tables_status(USER, "", b""))
show("interserver, unauthenticated", tables_status(USER_INTERSERVER_MARKER, "", INTERSERVER))
PYEOF
