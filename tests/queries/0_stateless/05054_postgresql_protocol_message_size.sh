#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the PostgreSQL compatibility port is not enabled in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Authentication messages are parsed before the client is authenticated, so a field of a message
# must not be allocated at the size the client declares for it, and parsing a message must not
# continue past the size the client declared for the message.

USER_SCRAM="user_scram_${CLICKHOUSE_DATABASE}"
USER_PLAIN="user_plain_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT --query "CREATE USER ${USER_SCRAM} IDENTIFIED WITH scram_sha256_password BY 'x'"
$CLICKHOUSE_CLIENT --query "CREATE USER ${USER_PLAIN} IDENTIFIED WITH plaintext_password BY 'x'"

CLICKHOUSE_PORT_POSTGRESQL="$CLICKHOUSE_PORT_POSTGRESQL" USER_SCRAM="$USER_SCRAM" USER_PLAIN="$USER_PLAIN" python3 - <<'PYTHON'
import os
import socket
import struct

port = int(os.environ["CLICKHOUSE_PORT_POSTGRESQL"])
user_scram = os.environ["USER_SCRAM"]
user_plain = os.environ["USER_PLAIN"]


def connect():
    sock = socket.create_connection(("127.0.0.1", port), timeout=30)
    sock.settimeout(30)
    return sock


def startup(sock, user):
    payload = ("user\x00" + user + "\x00\x00").encode()
    sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)


def outcome(sock):
    """Drain the reply and describe it: a rejected message gets an error response, an accepted one
    that is never completed leaves the server waiting for the payload."""
    data = b""
    while True:
        try:
            chunk = sock.recv(4096)
        except socket.timeout:
            return "TIMEOUT"
        except ConnectionResetError:
            break
        if not chunk:
            break
        data += chunk
    if data[0:1] == b"E":
        return "error response"
    if not data:
        return "NO REPLY"
    return "UNEXPECTED REPLY " + repr(data[:64])


def first_reply(sock):
    """Describe the first reply message, for a case where the server rejects the message but keeps
    the connection open, so waiting for it to close would only hit the timeout."""
    try:
        data = sock.recv(4096)
    except (socket.timeout, ConnectionResetError):
        return "TIMEOUT"
    if data[0:1] == b"E":
        return "error response"
    if not data:
        return "NO REPLY"
    return "UNEXPECTED REPLY " + repr(data[:64])


# A SASL initial response that declares a two-gigabyte mechanism in a thirty-byte message.
sock = connect()
startup(sock, user_scram)
sock.recv(4096)
body = b"SCRAM-SHA-256\x00" + struct.pack(">i", 0x7FFFFFFF)
sock.sendall(b"p" + struct.pack(">i", 4 + len(body)) + body)
print("SASL mechanism longer than the message:", outcome(sock))
sock.close()

# A password message that declares a small size and then streams a password without a terminator.
sock = connect()
startup(sock, user_plain)
sock.recv(4096)
sock.sendall(b"p" + struct.pack(">i", 4 + 10))
try:
    sock.sendall(b"x" * 200000)
except (BrokenPipeError, ConnectionResetError, socket.timeout):
    pass
print("password without a terminator:", outcome(sock))
sock.close()

# A well-formed exchange still works: authenticate and run a query.
def read_until_ready(sock):
    data = b""
    while not data.endswith(b"Z\x00\x00\x00\x05I"):
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    return data


sock = connect()
startup(sock, user_plain)
sock.recv(4096)
sock.sendall(b"p" + struct.pack(">i", 4 + 2) + b"x\x00")
read_until_ready(sock)
sock.sendall(b"Q" + struct.pack(">i", 4 + 9) + b"SELECT 1\x00")
data = read_until_ready(sock)
print("well-formed exchange:", "query result received" if b"C\x00\x00\x00\rSELECT 1\x00" in data else "UNEXPECTED REPLY " + repr(data[:96]))
sock.close()

# A `Query` that terminates its string early and smuggles a whole second `Query` into the tail of the
# same declared frame. The declared length is a frame boundary, so the tail must be rejected instead
# of being read as the next message.
sock = connect()
startup(sock, user_plain)
sock.recv(4096)
sock.sendall(b"p" + struct.pack(">i", 4 + 2) + b"x\x00")
read_until_ready(sock)
body = b"SELECT 1\x00" + b"SELECT 2\x00"
sock.sendall(b"Q" + struct.pack(">i", 4 + len(body)) + body)
print("query with a smuggled tail:", first_reply(sock))
sock.close()

# A `Query` that declares more bytes than the client sends and then closes the write side of the
# connection. The declared length is a frame boundary in both directions, so the message must be
# rejected instead of being executed with the part of the payload that did arrive.
sock = connect()
startup(sock, user_plain)
sock.recv(4096)
sock.sendall(b"p" + struct.pack(">i", 4 + 2) + b"x\x00")
read_until_ready(sock)
body = b"SELECT 1\x00"
sock.sendall(b"Q" + struct.pack(">i", 4 + len(body) + 1000) + body)
sock.shutdown(socket.SHUT_WR)
print("query shorter than declared:", outcome(sock))
sock.close()

# An oversized `Sync`, whose parser reads nothing at all: its payload must not survive the message
# boundary and be reinterpreted as the next message.
sock = connect()
startup(sock, user_plain)
sock.recv(4096)
sock.sendall(b"p" + struct.pack(">i", 4 + 2) + b"x\x00")
read_until_ready(sock)
body = b"Q" + struct.pack(">i", 4 + 9) + b"SELECT 2\x00"
sock.sendall(b"S" + struct.pack(">i", 4 + len(body)) + body)
print("oversized sync:", outcome(sock))
sock.close()
PYTHON

$CLICKHOUSE_CLIENT --query "DROP USER ${USER_SCRAM}"
$CLICKHOUSE_CLIENT --query "DROP USER ${USER_PLAIN}"
