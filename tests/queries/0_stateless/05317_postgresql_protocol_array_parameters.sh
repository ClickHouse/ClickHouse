#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the PostgreSQL compatibility port is not enabled in fasttest.

# A prepared statement of the extended query protocol may declare a parameter as a PostgreSQL array
# type (`int4[]`, `text[]`, ...), and the client then binds the array literal `{...}` to it. The value
# must reach the query as an array of the element type (`Nullable` only when an element is `NULL`), not
# as the string `'{...}'`, and a value that is not a well-formed rectangular array literal, or whose
# element does not fit the element type, is an error. A literal with explicit bounds (`[0:1]={1,2}`)
# is an error too: a ClickHouse array always starts at index 1.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The user name must be unique per test run, so that concurrent runs do not collide.
PG_USER="postgresql_user_05317_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "
DROP USER IF EXISTS ${PG_USER};
CREATE USER ${PG_USER} HOST IP '127.0.0.1' IDENTIFIED WITH no_password;
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


def until_ready():
    """Collect the data rows and the error messages up to `ReadyForQuery`."""
    rows = []
    errors = []
    while True:
        kind, body = receive_message()
        if kind == b"Z":
            return rows, errors
        if kind == b"D":
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
            rows.append(row)
        elif kind == b"E":
            fields = dict((f[:1], f[1:]) for f in body.split(b"\x00") if f)
            errors.append(fields.get(b"M", b"").decode())
        elif kind == b"R":
            (code,) = struct.unpack(">i", body[:4])
            if code == 3:
                send(b"p", cstring(""))


payload = cstring("user") + cstring(user) + cstring("database") + cstring(database) + b"\x00"
sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)
until_ready()


def run(query, oid, value):
    send(b"P", cstring("") + cstring(query) + struct.pack(">hi", 1, oid))
    encoded = value.encode()
    send(b"B", cstring("") + cstring("") + struct.pack(">hhi", 0, 1, len(encoded)) + encoded + struct.pack(">h", 0))
    send(b"E", cstring("") + struct.pack(">i", 0))
    send(b"S", b"")
    rows, errors = until_ready()
    for row in rows:
        print("\t".join(row))
    for error in errors:
        if "not a PostgreSQL array literal" in error:
            print("error: not a PostgreSQL array literal")
        elif "explicit array bounds" in error:
            print("error: explicit array bounds")
        elif "numeric array" in error:
            print("error: invalid numeric element")
        else:
            print("error")


INT2_ARRAY, INT4_ARRAY, INT8_ARRAY, BOOL_ARRAY = 1005, 1007, 1016, 1000
TEXT_ARRAY, NUMERIC_ARRAY, DATE_ARRAY = 1009, 1231, 1182

print("--- one dimension")
run("SELECT arraySum($1), toTypeName($1)", INT4_ARRAY, "{1,2,3}")
run("SELECT isNull($1[2]), toTypeName($1)", INT4_ARRAY, "{1,NULL}")
run("SELECT toTypeName($1), length($1), $1[2], isNull($1[3]), $1[4]", TEXT_ARRAY, '{a, "b,c" ,NULL,"NULL"}')
run("SELECT $1, toTypeName($1)", BOOL_ARRAY, "{t,f}")
run("SELECT $1[2], toTypeName($1)", DATE_ARRAY, "{2026-01-01,2026-10-02}")
run("SELECT toString($1), toTypeName($1)", NUMERIC_ARRAY, "{1.5, 2.25, NULL}")
run("SELECT length($1), toTypeName($1)", INT4_ARRAY, "{}")

print("--- several dimensions")
run("SELECT arrayFlatten($1), toTypeName($1)", INT8_ARRAY, " {{1,2},{3,4}} ")
run("SELECT $1[1][1], $1[2][1], toTypeName($1)", TEXT_ARRAY, '{{"x\\"y",b},{c,d}}')
run("SELECT length($1), length($1[1]), length($1[2]), toTypeName($1)", INT4_ARRAY, "{{},{ }}")

print("--- not an array of the element type")
run("SELECT $1", INT4_ARRAY, "1,2")
run("SELECT $1", INT4_ARRAY, "{1,2")
run("SELECT $1", INT4_ARRAY, "{1,{2}}")
run("SELECT $1", INT4_ARRAY, "{{1},{2,3}}")
run("SELECT $1", INT4_ARRAY, "{{},{1}}")
run("SELECT $1", INT4_ARRAY, "{{},{{}}}")
run("SELECT $1", INT4_ARRAY, "{1,,2}")
run("SELECT $1", INT4_ARRAY, "{1} 2")
run("SELECT $1", INT4_ARRAY, "{{{{{{{1}}}}}}}")
run("SELECT $1", INT2_ARRAY, "{70000}")
run("SELECT $1", NUMERIC_ARRAY, "{1.5,abc}")

print("--- explicit bounds of an array whose lower bound is not 1")
run("SELECT $1", INT4_ARRAY, "[0:1]={1,2}")
run("SELECT $1", INT4_ARRAY, " [2:3][4:5]={{1,2},{3,4}}")

print("--- the connection is still usable")
run("SELECT arraySum($1)", INT4_ARRAY, "{40,2}")
PYTHON

${CLICKHOUSE_CLIENT} -q "DROP USER ${PG_USER}"
