#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the PostgreSQL compatibility port is not enabled in fasttest.

# The data of a `COPY ... FROM STDIN` ends at a line holding only `\.`, which `psql` before version 18
# sends as part of the data: the line is not a row, and what follows it is ignored. In the CSV format
# the marker is only recognized outside of a quoted value. The data is sent on a raw socket, so that the
# test does not depend on the version of `psql`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run, so that concurrent runs do not collide.
PG_USER="postgresql_user_05318_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
CREATE TABLE copy_marker (a String, b String) ENGINE = Memory;
GRANT SELECT, INSERT, TRUNCATE ON ${CLICKHOUSE_DATABASE}.copy_marker TO ${PG_USER};
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


def until_ready(copy_data=None):
    """Answer a `CopyInResponse` with the data split into the given frames, and collect the
    `CommandComplete` tags, the data rows and the error messages up to `ReadyForQuery`."""
    replies = []
    while True:
        kind, body = receive_message()
        if kind == b"Z":
            return replies
        if kind == b"G":
            for frame in copy_data:
                send(b"d", frame)
            send(b"c", b"")
        elif kind == b"C":
            replies.append(body.rstrip(b"\x00").decode())
        elif kind == b"D":
            (count,) = struct.unpack(">h", body[:2])
            offset = 2
            row = []
            for _ in range(count):
                (length,) = struct.unpack(">i", body[offset : offset + 4])
                offset += 4
                row.append(body[offset : offset + length].decode())
                offset += length
            replies.append("\t".join(row))
        elif kind == b"E":
            fields = dict((f[:1], f[1:]) for f in body.split(b"\x00") if f)
            replies.append("error: " + fields.get(b"M", b"").decode().splitlines()[0])
        elif kind == b"R":
            (code,) = struct.unpack(">i", body[:4])
            if code == 3:
                send(b"p", cstring(""))


def query(text, copy_data=None):
    send(b"Q", cstring(text))
    for reply in until_ready(copy_data):
        if not reply.startswith("SELECT ") and not reply.startswith("TRUNCATE"):
            print(reply)


payload = cstring("user") + cstring(user) + cstring("database") + cstring(database) + b"\x00"
sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)
until_ready()


def copy(statement, frames):
    print("---", statement, repr(b"".join(frames).decode()))
    query("TRUNCATE TABLE copy_marker")
    query(statement, frames)
    query("SELECT replaceAll(a, '\n', '|'), b FROM copy_marker ORDER BY a, b")


copy("COPY copy_marker FROM STDIN", [b"x\ty\n", b"\\.\n"])
copy("COPY copy_marker FROM STDIN", [b"x\ty\n\\.\nignored\tafter the marker\n"])
copy("COPY copy_marker FROM STDIN", [b"x\ty\n\\.\r\n"])
copy("COPY copy_marker FROM STDIN", [b"x\ty\n\\.\n"])
# The marker split across frames, and without a line feed after it.
copy("COPY copy_marker FROM STDIN", [b"x\ty\n\\", b".", b"\n"])
copy("COPY copy_marker FROM STDIN", [b"x\ty\n\\."])
# A line starting with an escaped backslash is data.
copy("COPY copy_marker FROM STDIN", [b"\\\\.\ty\n"])
copy("COPY copy_marker FROM STDIN WITH (FORMAT csv)", [b"x,y\n\\.\n"])
# Inside a quoted value of the CSV format, the line is data.
copy("COPY copy_marker FROM STDIN WITH (FORMAT csv)", [b'"x\n\\.\n",y\n\\.\n'])
PYTHON

${CLICKHOUSE_CLIENT} -q "
DROP TABLE copy_marker;
DROP USER ${PG_USER};
"
