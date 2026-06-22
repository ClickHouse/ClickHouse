#!/usr/bin/env python3
"""Test that the MySQL handshake reads the `auth_response` length as an unsigned byte.

The `CLIENT_SECURE_CONNECTION` path of `HandshakeResponse::readPayloadImpl` used to read
the `auth_response` length into a signed `char` and widen it with `static_cast<unsigned int>`.
A length byte `>= 128` (high bit set) was therefore sign-extended into a multi-gigabyte value,
so such a response could not round-trip at all and the connection failed before authentication.

This sends a handshake response with an `auth_response` whose length byte has the high bit set
and the `mysql_native_password` plugin selected. The server now parses the field correctly and
hands the full N bytes to the auth plugin, which rejects it with
"Wrong size of auth response. Expected: 20 bytes, received: N bytes." — proving the length byte
was read as N rather than sign-extended. Before the fix the parse step itself failed with an
allocation / read error and a different message.

https://github.com/ClickHouse/ClickHouse/pull/107384
"""

import os
import socket
import struct

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT_MYSQL = int(os.environ.get("CLICKHOUSE_PORT_MYSQL", 9004))
CLICKHOUSE_USER = os.environ.get("MYSQL_CLIENT_CLICKHOUSE_USER", "default")

# Capability flags (see src/Core/MySQL/PacketsGeneric.h).
CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_SECURE_CONNECTION = 0x00008000
CLIENT_PLUGIN_AUTH = 0x00080000


def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed by server")
        data += chunk
    return data


def read_packet(sock):
    """Read one MySQL wire packet, returning (sequence_id, payload)."""
    header = recv_exact(sock, 4)
    length = header[0] | (header[1] << 8) | (header[2] << 16)
    sequence_id = header[3]
    return sequence_id, recv_exact(sock, length)


def send_packet(sock, sequence_id, payload):
    length = len(payload)
    header = struct.pack("<I", length)[:3] + bytes([sequence_id])
    sock.sendall(header + payload)


def parse_err_message(payload):
    """Return the human-readable message of an ERR packet, or None if it is not one."""
    if not payload or payload[0] != 0xFF:
        return None
    # 0xFF, error_code (2 bytes), '#', sql_state (5 bytes), then the message.
    return payload[9:].decode("utf-8", errors="replace")


def try_auth_response_length(auth_response_size):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT_MYSQL))
    try:
        # Consume the server's initial Handshake packet.
        read_packet(sock)

        username = CLICKHOUSE_USER.encode()
        auth_plugin_name = b"mysql_native_password"
        auth_response = b"x" * auth_response_size

        capability_flags = (
            CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH
        )

        payload = bytearray()
        payload += struct.pack("<I", capability_flags)
        payload += struct.pack("<I", 0xFFFFFF)  # max_packet_size
        payload += bytes([33])  # character_set (utf8_general_ci)
        payload += b"\x00" * 23  # reserved
        payload += username + b"\x00"
        # CLIENT_SECURE_CONNECTION branch: one length byte followed by the data.
        payload += bytes([auth_response_size & 0xFF])
        payload += auth_response
        payload += auth_plugin_name + b"\x00"

        send_packet(sock, 1, bytes(payload))

        _seq, response = read_packet(sock)
        message = parse_err_message(response)
        expected = f"received: {auth_response_size} bytes"
        if message is not None and expected in message:
            print(f"auth_response length {auth_response_size} read correctly")
        else:
            print(f"UNEXPECTED for length {auth_response_size}: {message!r}")
    finally:
        sock.close()


def main():
    # Lengths with the high bit set (>= 128) used to be sign-extended before the fix.
    for size in (128, 200, 255):
        try_auth_response_length(size)


if __name__ == "__main__":
    main()
