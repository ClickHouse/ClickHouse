#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the PostgreSQL compatibility port is not enabled in fasttest.

# `HEADER` of `COPY ... TO STDOUT` sends the column names line even when the result has no rows, and
# inside the parenthesized option list `HEADER` accepts only a boolean value: `WITH (HEADER csv)` is
# rejected rather than read as `HEADER` followed by `FORMAT csv`, while the legacy unparenthesized
# grammar still allows another option right after `HEADER`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run, so that concurrent runs do not collide.
PG_USER="postgresql_user_05333_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
CREATE TABLE copy_header (a String, b UInt32) ENGINE = Memory;
GRANT SELECT, INSERT ON ${CLICKHOUSE_DATABASE}.copy_header TO ${PG_USER};
"

CLICKHOUSE_PORT_POSTGRESQL="$CLICKHOUSE_PORT_POSTGRESQL" PG_USER="$PG_USER" PG_DATABASE="$CLICKHOUSE_DATABASE" python3 - <<'PYTHON'
import os
import socket
import struct

port = int(os.environ["CLICKHOUSE_PORT_POSTGRESQL"])
user = os.environ["PG_USER"]
database = os.environ["PG_DATABASE"]

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


def until_ready(copy_data=b""):
    """Collect the `CommandComplete` tags, the copied-out data and the error messages up to
    `ReadyForQuery`, answering a `CopyInResponse` with the given data."""
    replies = []
    while True:
        kind, body = receive_message()
        if kind == b"Z":
            return replies
        if kind == b"G":
            send(b"d", copy_data)
            send(b"c", b"")
        elif kind == b"d":
            replies.append("data: " + repr(body.decode()))
        elif kind == b"C":
            replies.append(body.rstrip(b"\x00").decode())
        elif kind == b"E":
            fields = dict((f[:1], f[1:]) for f in body.split(b"\x00") if f)
            replies.append("error: " + fields.get(b"M", b"").decode().splitlines()[0])
        elif kind == b"R":
            (code,) = struct.unpack(">i", body[:4])
            if code == 3:
                send(b"p", cstring(""))


def query(text, copy_data=b""):
    print("---", text)
    send(b"Q", cstring(text))
    for reply in until_ready(copy_data):
        print(reply)


payload = cstring("user") + cstring(user) + cstring("database") + cstring(database) + b"\x00"
sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)
until_ready()

# An empty result still gets its header line.
query("COPY copy_header TO STDOUT WITH (FORMAT csv, HEADER)")
query("COPY copy_header TO STDOUT WITH (FORMAT text, HEADER true)")
query("COPY (SELECT a FROM copy_header) TO STDOUT CSV HEADER")
# Without `HEADER` an empty result is an empty stream.
query("COPY copy_header TO STDOUT WITH (FORMAT csv)")

query("COPY copy_header FROM STDIN WITH (FORMAT csv)", b"x,1\n")
query("COPY copy_header TO STDOUT WITH (FORMAT csv, HEADER on)")

# Inside parentheses, the value of `HEADER` must be a boolean.
query("COPY copy_header TO STDOUT WITH (HEADER csv)")
query("COPY copy_header TO STDOUT WITH (FORMAT csv, HEADER DELIMITER ',')")
query("COPY copy_header TO STDOUT WITH (FORMAT csv, HEADER false)")
# The legacy grammar: `HEADER` takes no value and may be followed by another option.
query("COPY copy_header TO STDOUT HEADER CSV")
query("COPY copy_header TO STDOUT CSV HEADER DELIMITER ','")

# The connection is still usable.
query("COPY copy_header TO STDOUT")
PYTHON

${CLICKHOUSE_CLIENT} -q "
DROP TABLE copy_header;
DROP USER ${PG_USER};
"
