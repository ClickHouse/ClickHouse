#!/usr/bin/env python3
"""Regression test for the MySQL handshake integer underflow (PR #115776).

MySQLHandler::finishHandshake reads the first bytes to learn the declared payload size,
then reads the remaining `packet_size - pos` bytes. The first socket read can return more
bytes than the declared packet, so when the client announces a payload smaller than what
it already sent, pos > packet_size and the unsigned subtraction underflows, turning the
follow-up read into an unbounded pre-auth read that buffers socket data until EOF.

We send, in a single write, a header declaring a 5-byte payload (packet_size = 4 + 5 = 9)
followed by 36 bytes total, so the server's first read leaves pos = 36 > packet_size. The
fixed server rejects the malformed packet and closes the connection immediately; the
unfixed server stays blocked in the unbounded read. We assert the connection is closed
without the server waiting for more data.
"""

import os
import socket
import struct

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT_MYSQL = int(os.environ.get("CLICKHOUSE_PORT_MYSQL", 9004))


def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed by server")
        data += chunk
    return data


def read_packet(sock):
    header = recv_exact(sock, 4)
    length = header[0] | (header[1] << 8) | (header[2] << 16)
    return recv_exact(sock, length)


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT_MYSQL))
    try:
        read_packet(sock)

        declared_payload = 5
        header = struct.pack("<I", declared_payload)[:3] + bytes([1])  # 3-byte length + seq id
        blob = header + b"\x00" * 32  # 4 + 32 = 36 bytes total
        sock.sendall(blob)

        sock.settimeout(5)
        try:
            data = sock.recv(1)
        except (ConnectionResetError, BrokenPipeError):
            data = b""
        except socket.timeout:
            raise AssertionError(
                "server kept the connection open and did not reject the malformed "
                "handshake (integer underflow not fixed)"
            )
        assert data == b"", f"unexpected data instead of connection close: {data!r}"
        print("OK: malformed handshake rejected")
    finally:
        sock.close()


if __name__ == "__main__":
    main()