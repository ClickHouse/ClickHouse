#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the MySQL compatibility port and TLS are not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The handshake response is read before the client is authenticated, so it must be bounded: on the
# TLS path it was read through a buffer that follows a chain of maximum-size packets as one logical
# message, and the fields inside it are read up to a terminator, so a peer could stream data and make
# the server grow memory without a limit.

CLICKHOUSE_PORT_MYSQL="$CLICKHOUSE_PORT_MYSQL" python3 - <<'PYTHON'
import os
import socket
import ssl
import struct

port = int(os.environ["CLICKHOUSE_PORT_MYSQL"])

CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_SSL = 0x00000800
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_PLUGIN_AUTH = 0x00080000

CAPABILITIES = CLIENT_PROTOCOL_41 | CLIENT_SSL | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
# capability flags, max packet size, character set and 23 reserved bytes: the fixed part of both the
# SSL request and the handshake response.
FIXED_PART = struct.pack("<IIB", CAPABILITIES, 0, 45) + b"\x00" * 23


def connect():
    sock = socket.create_connection(("127.0.0.1", port), timeout=30)
    sock.settimeout(30)
    return sock


def packet(sequence_id, payload):
    return struct.pack("<I", len(payload))[:3] + bytes([sequence_id]) + payload


def read_packet(sock):
    header = b""
    while len(header) < 4:
        header += sock.recv(4 - len(header))
    size = int.from_bytes(header[:3], "little")
    payload = b""
    while len(payload) < size:
        payload += sock.recv(size - len(payload))
    return payload


def outcome(sock):
    """A bounded read rejects the response and closes the connection; an unbounded one keeps waiting."""
    while True:
        try:
            if not sock.recv(4096):
                return "rejected"
        except socket.timeout:
            return "TIMEOUT"
        except (ConnectionResetError, OSError):
            return "rejected"


def start_tls(sock):
    sock.sendall(packet(1, FIXED_PART))
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    secure = context.wrap_socket(sock)
    secure.settimeout(30)
    return secure


# A handshake response that declares a payload far larger than any real one.
sock = connect()
read_packet(sock)
sock.sendall(struct.pack("<I", 100000)[:3] + bytes([1]) + b"x" * 100)
print("declared payload too large:", outcome(sock))
sock.close()

# A handshake response over TLS whose user name never ends.
sock = connect()
read_packet(sock)
secure = start_tls(sock)
secure.sendall(struct.pack("<I", 0xFFFFFF)[:3] + bytes([2]) + FIXED_PART)
try:
    for _ in range(20):
        secure.sendall(b"x" * 16384)
except (BrokenPipeError, ConnectionResetError, ssl.SSLError, socket.timeout, OSError):
    pass
print("user name without a terminator:", outcome(secure))
secure.close()

# A handshake response over TLS that declares a maximum-size payload but carries a complete,
# parseable prefix: the packet is truncated, so it must be rejected rather than accepted as if the
# declared payload had ended.
sock = connect()
read_packet(sock)
secure = start_tls(sock)
prefix = FIXED_PART + b"default\x00" + b"\x00" + b"mysql_native_password\x00"
secure.sendall(struct.pack("<I", 0xFFFFFF)[:3] + bytes([2]) + prefix)
print("truncated packet with a complete prefix:", outcome(secure))
secure.close()

# A well-formed handshake over TLS still authenticates.
sock = connect()
read_packet(sock)
secure = start_tls(sock)
secure.sendall(packet(2, FIXED_PART + b"default\x00" + b"\x00" + b"mysql_native_password\x00"))
reply = read_packet(secure)
print("well-formed handshake:", "authenticated" if reply[0:1] == b"\x00" else "UNEXPECTED REPLY " + repr(reply[:64]))
secure.close()
PYTHON
