#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the PostgreSQL compatibility port is not enabled in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A simple query without statements (whitespace, semicolons and comments only) is answered with
# `EmptyQueryResponse` ('I') followed by `ReadyForQuery` ('Z'), as in PostgreSQL. pgx v5 pings the
# server with `-- ping`. An unterminated block comment is a syntax error ('E').

PG_USER="postgresql_user_05365_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
"

CLICKHOUSE_PORT_POSTGRESQL="$CLICKHOUSE_PORT_POSTGRESQL" PG_USER="$PG_USER" PG_DATABASE="$CLICKHOUSE_DATABASE" python3 - <<'PYTHON'
import os
import socket
import struct
import sys

sock = socket.create_connection(("127.0.0.1", int(os.environ["CLICKHOUSE_PORT_POSTGRESQL"])), timeout=60)
sock.settimeout(60)
buffer = b""


def read_exact(n):
    global buffer
    while len(buffer) < n:
        chunk = sock.recv(65536)
        if not chunk:
            print("connection closed")
            sys.exit(0)
        buffer += chunk
    data, buffer = buffer[:n], buffer[n:]
    return data


def read_until_ready():
    """Returns the types of the messages up to and including `ReadyForQuery`."""
    types = []
    while True:
        header = read_exact(5)
        read_exact(struct.unpack(">i", header[1:5])[0] - 4)
        types.append(chr(header[0]))
        if header[0:1] == b"Z":
            return types


payload = ("user\x00" + os.environ["PG_USER"] + "\x00database\x00" + os.environ["PG_DATABASE"] + "\x00\x00").encode()
sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)
read_until_ready()

cases = [
    ("line comment", "-- ping"),
    ("block comment", "/* ping */"),
    ("nested block comment", "/* a /* b */ c */"),
    ("empty statements and comments", "; -- a\n;/* b */ ;"),
    ("comment mentioning the catalog", "-- pg_catalog.pg_type"),
    ("semicolon", ";"),
    ("empty string", ""),
    ("whitespace", " \t\n "),
    ("comment before a statement", "-- c\nSELECT 1"),
    ("comment after a statement", "SELECT 1; -- c"),
    ("unterminated block comment", "/* x"),
    ("unterminated nested block comment", "/* a /* b */"),
    ("connection still usable", "SELECT 1"),
]

for label, text in cases:
    body = text.encode() + b"\x00"
    sock.sendall(b"Q" + struct.pack(">i", 4 + len(body)) + body)
    print(label + ":", " ".join(read_until_ready()))

sock.sendall(b"X" + struct.pack(">i", 4))
sock.close()
PYTHON

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}"
