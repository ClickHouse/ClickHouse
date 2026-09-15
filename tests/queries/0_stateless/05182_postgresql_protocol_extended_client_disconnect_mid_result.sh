#!/usr/bin/env bash

# The extended-query counterpart of `05099_postgresql_protocol_client_disconnect_mid_result`: a
# client that goes away in the middle of an `Execute` result makes the next write to the socket
# fail, which cancels the output buffer. The handler must then drop the connection instead of
# trying to deliver `ErrorResponse` into the canceled buffer (that used to be a logical error,
# `Cannot write to canceled buffer`).
#
# `psql` cannot be used here because it drives the simple-query protocol for `-c`, so the client is
# a minimal PostgreSQL wire protocol implementation that sends `Parse`/`Bind`/`Execute`/`Sync` and
# then aborts the connection.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

PG_USER="postgresql_user_05182_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

# Every row is sent to the client as soon as it is produced, so the server keeps writing to the
# socket long after the client is gone.
MARKER="client_disconnect_05182_${CLICKHOUSE_DATABASE}"
LONG_QUERY="SELECT '${MARKER}', sleepEachRow(0.1) FROM numbers(3000)
    SETTINGS max_block_size = 1, max_threads = 1, max_execution_time = 0"

python3 - "127.0.0.1" "${CLICKHOUSE_PORT_POSTGRESQL}" "${CLICKHOUSE_DATABASE}" "${PG_USER}" "${LONG_QUERY}" <<'PYTHON'
import socket
import struct
import sys

host, port, database, user, query = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5]


def message(tag, body):
    return tag + struct.pack("!I", len(body) + 4) + body


def cstring(value):
    return value.encode() + b"\x00"


sock = socket.create_connection((host, port), timeout = 60)

# StartupMessage carries no tag, only the protocol version 3.0 and the connection parameters.
startup = struct.pack("!I", 196608) + cstring("user") + cstring(user) + cstring("database") + cstring(database) + b"\x00"
sock.sendall(struct.pack("!I", len(startup) + 4) + startup)


def receive_message():
    header = b""
    while len(header) < 5:
        chunk = sock.recv(5 - len(header))
        if not chunk:
            raise RuntimeError("the server closed the connection")
        header += chunk
    tag = header[:1]
    size = struct.unpack("!I", header[1:])[0] - 4
    body = b""
    while len(body) < size:
        chunk = sock.recv(size - len(body))
        if not chunk:
            raise RuntimeError("the server closed the connection")
        body += chunk
    return tag, body


# Authentication and the parameter status data, up to the first `ReadyForQuery`.
while True:
    tag, _ = receive_message()
    if tag == b"E":
        raise RuntimeError("the server refused the connection")
    if tag == b"Z":
        break

sock.sendall(message(b"P", cstring("") + cstring(query) + struct.pack("!H", 0)))
sock.sendall(message(b"B", cstring("") + cstring("") + struct.pack("!HHH", 0, 0, 0)))
sock.sendall(message(b"E", cstring("") + struct.pack("!I", 0)))
sock.sendall(message(b"S", b""))

# Wait until the result is actually streaming, then abort the connection: `SO_LINGER` with a zero
# timeout makes `close` send `RST` instead of `FIN`, so the server sees the failure on its next
# write without the client having to send `Terminate` or `CancelRequest`.
while True:
    tag, _ = receive_message()
    if tag == b"D":
        break
    if tag == b"E":
        raise RuntimeError("the server failed the statement")

sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
sock.close()
PYTHON

function count_running()
{
    ${CLICKHOUSE_CLIENT} -q "
        SELECT count() FROM system.processes
        WHERE query LIKE '%${MARKER}%' AND query NOT LIKE '%system.processes%'"
}

for _ in {1..300}
do
    [[ "$(count_running)" == "0" ]] && break
    sleep 0.1
done

echo "--- after the client is gone, the statement is gone too"
count_running

echo "--- and the server did not try to write into the canceled socket buffer"
${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS text_log"
${CLICKHOUSE_CLIENT} -q "
    SELECT count() FROM system.text_log
    WHERE logger_name = 'PostgreSQLHandler' AND message LIKE '%Cannot write to canceled buffer%'"

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}"
