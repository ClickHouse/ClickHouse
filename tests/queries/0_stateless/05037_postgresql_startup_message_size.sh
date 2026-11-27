#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the PostgreSQL compatibility port is not enabled in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The startup message is processed before authentication, so its size must be bounded,
# and parsing must not read past the declared size of the message.

CLICKHOUSE_PORT_POSTGRESQL="$CLICKHOUSE_PORT_POSTGRESQL" python3 - <<'PYTHON'
import os
import socket
import struct

port = int(os.environ["CLICKHOUSE_PORT_POSTGRESQL"])

def connect():
    sock = socket.create_connection(("127.0.0.1", port), timeout=30)
    sock.settimeout(30)
    return sock

def receive_exact(sock, size):
    payload = b""
    while len(payload) < size:
        chunk = sock.recv(size - len(payload))
        if not chunk:
            raise EOFError("connection closed before the full PostgreSQL message arrived")
        payload += chunk
    return payload

def read_message(sock):
    message_type = receive_exact(sock, 1)
    if not message_type:
        return b"", b""
    length = struct.unpack(">i", receive_exact(sock, 4))[0]
    payload = receive_exact(sock, length - 4)
    return message_type, payload

def read_error(sock):
    """Read the reply and return the human-readable error message, if any."""
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
    if not data:
        return "NO REPLY"
    if data[0:1] != b"E":
        return "UNEXPECTED REPLY"
    if b"Can't correctly handle Startup message" in data:
        return "ERROR: Can't correctly handle Startup message"
    return "ERROR: " + data.decode("utf-8", "replace")

# A startup message with an absurdly large declared size must be rejected without allocating it.
sock = connect()
sock.sendall(struct.pack(">ii", 1000000000, 196608))
print("huge declared size:", read_error(sock))
sock.close()

# A startup message that declares a small size, but then streams an unterminated parameter name,
# must be rejected as well: the parser must not keep reading past the declared size.
sock = connect()
sock.sendall(struct.pack(">ii", 30, 196608))
try:
    sock.sendall(b"x" * 65536)
except (BrokenPipeError, ConnectionResetError, socket.timeout):
    pass
print("unterminated parameter:", read_error(sock))
sock.close()

# A well-formed startup message is still accepted (the reply is an authentication request).
sock = connect()
payload = b"user\x00default\x00\x00"
sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)
reply = sock.recv(1)
print("well-formed message:", "authentication request" if reply == b"R" else "unexpected reply " + repr(reply))
sock.close()

# Common spelling variants of UTF8 remain accepted.
sock = connect()
payload = b"user\x00default\x00client_encoding\x00UTF-8\x00\x00"
sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)
reply = sock.recv(1)
print("UTF-8 alias:", "authentication request" if reply == b"R" else "unexpected reply " + repr(reply))
sock.close()

# `UNICODE` is a documented PostgreSQL alias for UTF8, matched case-insensitively.
sock = connect()
payload = b"user\x00default\x00client_encoding\x00unicode\x00\x00"
sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)
reply = sock.recv(1)
print("UNICODE alias:", "authentication request" if reply == b"R" else "unexpected reply " + repr(reply))
sock.close()

# ClickHouse serializes PostgreSQLWire text as UTF-8 and does not transcode result data. Refuse a
# different client encoding instead of advertising it and returning bytes in the wrong encoding.
sock = connect()
payload = b"user\x00default\x00client_encoding\x00LATIN1\x00\x00"
sock.sendall(struct.pack(">ii", 8 + len(payload), 196608) + payload)
message_type, error = read_message(sock)
if message_type == b"E":
    print("unsupported client encoding:", "Only UTF8 client_encoding is supported." if b"Only UTF8 client_encoding is supported." in error else "unexpected error")
else:
    print("unsupported client encoding:", "unexpectedly accepted")
sock.close()
PYTHON
