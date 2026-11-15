#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the PostgreSQL compatibility port is not enabled in fasttest.

# Three details of the PostgreSQL wire protocol:
# - a `numeric` prepared-statement parameter (and an element of a `numeric[]` one) whose scale covers all
#   76 significant digits, like `0.0...01`, fits `Decimal256(76)` and must not be rejected as too precise;
# - a `COPY ... TO STDOUT` cancelled by `KILL QUERY` is reported with SQLSTATE `57014 query_canceled`, not
#   as an ordinary execution error, and the connection stays usable;
# - a statement that merely mentions `pg_` refreshes the OIDs of the emulated catalog, and that internal
#   refresh must not fail because of the limits of the session (here `max_rows_in_set`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run, so that concurrent runs do not collide.
PG_USER="postgresql_user_05360_${CLICKHOUSE_DATABASE}"
MARKER="copy_cancel_05360_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

CLICKHOUSE_PORT_POSTGRESQL="$CLICKHOUSE_PORT_POSTGRESQL" PG_USER="$PG_USER" PG_DATABASE="$CLICKHOUSE_DATABASE" \
    MARKER="$MARKER" CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT" python3 - <<'PYTHON'
import os
import socket
import struct
import subprocess
import threading
import time

port = int(os.environ["CLICKHOUSE_PORT_POSTGRESQL"])
user = os.environ["PG_USER"]
database = os.environ["PG_DATABASE"]
marker = os.environ["MARKER"]
client = os.environ["CLICKHOUSE_CLIENT"]

sock = socket.create_connection(("127.0.0.1", port), timeout=60)
sock.settimeout(60)
buffer = b""


def receive_exact(size):
    global buffer
    while len(buffer) < size:
        chunk = sock.recv(65536)
        if not chunk:
            raise RuntimeError("connection closed")
        buffer += chunk
    data, buffer = buffer[:size], buffer[size:]
    return data


def receive_message():
    kind = receive_exact(1)
    (size,) = struct.unpack(">i", receive_exact(4))
    return kind, receive_exact(size - 4)


def send(kind, body):
    sock.sendall(kind + struct.pack(">i", 4 + len(body)) + body)


def cstring(text):
    return text.encode() + b"\x00"


def parse_row(body):
    (count,) = struct.unpack(">h", body[:2])
    offset = 2
    row = []
    for _ in range(count):
        (length,) = struct.unpack(">i", body[offset : offset + 4])
        offset += 4
        if length < 0:
            row.append("NULL")
        else:
            row.append(body[offset : offset + length].decode())
            offset += length
    return row


def until_ready():
    """Print the data rows and the SQLSTATE of the errors up to `ReadyForQuery`."""
    while True:
        kind, body = receive_message()
        if kind == b"Z":
            return
        if kind == b"D":
            print("\t".join(parse_row(body)))
        elif kind == b"E":
            fields = dict((f[:1], f[1:]) for f in body.split(b"\x00") if f)
            print("error", fields.get(b"C", b"").decode())
        elif kind == b"R":
            (code,) = struct.unpack(">i", body[:4])
            if code == 3:
                send(b"p", cstring(""))


def query(text):
    send(b"Q", cstring(text))
    until_ready()


def run(text, oid, value):
    send(b"P", cstring("") + cstring(text) + struct.pack(">hi", 1, oid))
    encoded = value.encode()
    send(b"B", cstring("") + cstring("") + struct.pack(">hhi", 0, 1, len(encoded)) + encoded + struct.pack(">h", 0))
    send(b"E", cstring("") + struct.pack(">i", 0))
    send(b"S", b"")
    until_ready()


payload = cstring("user") + cstring(user) + cstring("database") + cstring(database) + b"\x00"
sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)
until_ready()

NUMERIC, NUMERIC_ARRAY = 1700, 1231
tiny = "0." + "0" * 75 + "1"

print("--- numeric with the scale covering all 76 digits")
run(f"SELECT toTypeName($1), toString($1) = '{tiny}'", NUMERIC, tiny)
run("SELECT toTypeName($1), length($1)", NUMERIC_ARRAY, "{" + tiny + "}")
print("--- one digit more is still too precise")
run("SELECT $1", NUMERIC, "0." + "0" * 76 + "1")

print("--- a cancelled COPY TO STDOUT")
filter = f"query LIKE '%{marker}%' AND query NOT LIKE '%system.processes%'"


def kill_copy():
    """Wait for the copy to start running on the server, then kill it."""
    for _ in range(600):
        running = subprocess.run(
            client.split() + ["-q", f"SELECT count() FROM system.processes WHERE {filter}"],
            check=True, capture_output=True, text=True,
        ).stdout.strip()
        if running != "0":
            break
        time.sleep(0.1)
    subprocess.run(client.split() + ["-q", f"KILL QUERY WHERE {filter} SYNC FORMAT Null"], check=True)


killer = threading.Thread(target=kill_copy)
killer.start()
query(
    f"COPY (SELECT '{marker}', sleepEachRow(0.1) FROM numbers(3000) "
    "SETTINGS max_block_size = 1, max_threads = 1, max_execution_time = 0) TO STDOUT"
)
killer.join()
query("SELECT 42")

print("--- the catalog refresh does not inherit the session limits")
query("SELECT 'pg_1'")
query("SET max_rows_in_set = 1")
query("SELECT 'pg_2'")
query("SET max_rows_in_set = 0")
query("SELECT count() > 0 FROM pg_catalog.pg_namespace")
PYTHON

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}"
