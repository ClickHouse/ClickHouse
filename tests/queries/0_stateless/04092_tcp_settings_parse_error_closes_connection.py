#!/usr/bin/env python3
"""Test that a settings parsing error during query processing closes the
connection instead of leaving the TCP stream desynchronized.

When `BaseSettings::read` encounters an unknown setting with the IMPORTANT
flag, it throws without consuming the setting value from the buffer. This
leaves the read buffer in an inconsistent state. If the server tried to
reuse the connection, subsequent reads would misinterpret leftover bytes as
new packets — the specific null-dereference path this caused was fixed in
https://github.com/ClickHouse/ClickHouse/pull/94434, but the underlying
buffer desynchronization remained.

This fix makes `TCPHandler` detect that `query_context` was never created
(the exception happened during initial parsing) and close the connection,
which is the only safe option when the input buffer state is unknown.
"""

import os
import random
import socket
import struct
import subprocess
import sys

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT_TCP", 9000))
CLICKHOUSE_CLIENT = os.environ.get("CLICKHOUSE_CLIENT", "clickhouse-client")

# -- Minimal native protocol helpers -----------------------------------------

def write_varuint(value):
    result = bytearray()
    while value > 0x7F:
        result.append(0x80 | (value & 0x7F))
        value >>= 7
    result.append(value & 0x7F)
    return bytes(result)

def write_string(s):
    if isinstance(s, str):
        s = s.encode()
    return write_varuint(len(s)) + s

def read_varuint(sock):
    result, shift = 0, 0
    while True:
        b = sock.recv(1)
        if not b:
            raise ConnectionError("Connection closed")
        result |= (b[0] & 0x7F) << shift
        if not (b[0] & 0x80):
            return result
        shift += 7

def read_string(sock):
    n = read_varuint(sock)
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError
        data += chunk
    return data.decode("utf-8", errors="replace")

def recv_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise ConnectionError
        data += chunk
    return data

CLIENT_REVISION = 54440
CLIENT_NAME = "ClickHouse test"

def send_hello(sock):
    pkt = bytearray()
    pkt += write_varuint(0)
    pkt += write_string(CLIENT_NAME)
    pkt += write_varuint(25)
    pkt += write_varuint(1)
    pkt += write_varuint(CLIENT_REVISION)
    pkt += write_string("")
    pkt += write_string("default")
    pkt += write_string("")
    sock.sendall(pkt)

def recv_hello(sock):
    pkt_type = read_varuint(sock)
    if pkt_type == 2:
        code = struct.unpack("<I", recv_exact(sock, 4))[0]
        name = read_string(sock)
        message = read_string(sock)
        raise Exception(f"Server exception {code}: {name}: {message}")
    assert pkt_type == 0, f"Expected Hello, got {pkt_type}"
    read_string(sock)
    read_varuint(sock)
    read_varuint(sock)
    read_varuint(sock)
    if CLIENT_REVISION >= 54058:
        read_string(sock)
    if CLIENT_REVISION >= 54372:
        read_string(sock)
    if CLIENT_REVISION >= 54401:
        read_varuint(sock)

def build_client_info():
    buf = bytearray()
    buf += struct.pack("B", 1)
    buf += write_string("")
    buf += write_string("")
    buf += write_string("[::ffff:127.0.0.1]:0")
    buf += struct.pack("B", 1)
    buf += write_string("")
    buf += write_string("test")
    buf += write_string(CLIENT_NAME)
    buf += write_varuint(25)
    buf += write_varuint(1)
    buf += write_varuint(CLIENT_REVISION)
    buf += write_string("")
    buf += write_varuint(0)
    return bytes(buf)

def send_query(sock, settings_block, query_text="SELECT 1"):
    pkt = bytearray()
    pkt += write_varuint(1)
    pkt += write_string("")
    pkt += build_client_info()
    pkt += settings_block
    pkt += write_varuint(2)
    pkt += write_varuint(0)
    pkt += write_string(query_text)
    sock.sendall(pkt)

def send_empty_block(sock):
    pkt = bytearray()
    pkt += write_varuint(2)
    pkt += write_string("")
    pkt += write_varuint(0)
    pkt += write_varuint(0)
    pkt += write_varuint(0)
    sock.sendall(pkt)

def read_exception(sock):
    """Fully consume an Exception packet (after packet type is already read)."""
    code = struct.unpack("<I", recv_exact(sock, 4))[0]
    name = read_string(sock)
    message = read_string(sock)
    _stack_trace = read_string(sock)
    has_nested = recv_exact(sock, 1)[0]
    if has_nested:
        read_exception(sock)
    return code, message


def clickhouse_query(query):
    cmd = CLICKHOUSE_CLIENT.split() + ["--query", query]
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()


# -- Test --------------------------------------------------------------------

def test_connection_closed_after_bad_settings():
    """After a settings parse error, the server must close the connection
    rather than trying to preserve it for reuse.

    We verify this from both sides:
    - Client side: after consuming the Exception packet, recv() must return
      EOF or a connection error — not another valid server packet.
    - Server side: system.text_log must not show additional TCPHandler errors
      from reading a desynchronized buffer after the UNKNOWN_SETTING error.
    """
    setting_name = f"NONEXISTENT_{os.getpid()}_{random.randint(0, 2**31)}"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(10)
    sock.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT))

    try:
        send_hello(sock)
        recv_hello(sock)

        # Build settings block with our unique unknown IMPORTANT setting.
        settings = bytearray()
        settings += write_string(setting_name)
        settings += write_varuint(0x01)  # Flags: IMPORTANT
        settings += write_string("1")
        settings += write_string("")     # End marker

        send_query(sock, bytes(settings))
        send_empty_block(sock)

        # Server should respond with an exception for the unknown setting.
        pkt_type = read_varuint(sock)
        assert pkt_type == 2, f"Expected Exception packet (2), got {pkt_type}"
        code, message = read_exception(sock)
        assert setting_name in message, f"Unexpected error: {code}: {message}"

        # The server should close the connection after the parse error.
        # Verify we get EOF — not another valid server packet.
        sock.settimeout(5)
        try:
            data = sock.recv(1)
            assert not data, (
                f"Expected EOF after settings parse error, "
                f"but got data (first byte={data[0]})"
            )
        except (ConnectionResetError, ConnectionError, OSError):
            pass  # Also acceptable — server reset the connection.

    finally:
        sock.close()

    clickhouse_query("SYSTEM FLUSH LOGS text_log")

    # On the unfixed server, after the UNKNOWN_SETTING error the server
    # attempts to continue reading from the desync'd buffer (skipData, next
    # loop iteration), producing additional errors like CANNOT_READ_ALL_DATA
    # or UNEXPECTED_PACKET_FROM_CLIENT before closing the connection.
    # With the fix, the connection is closed immediately — no additional
    # errors appear between UNKNOWN_SETTING and "Done processing connection".
    #
    # We use the unique setting name to find the exact thread, then count
    # Error-level TCPHandler messages between our error and the next
    # "Done processing connection" on that thread.
    recent = "event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE"
    error_count = clickhouse_query(f"""
        WITH anchor AS (
            SELECT thread_id, event_time_microseconds AS t
            FROM system.text_log
            WHERE {recent}
            AND message LIKE '%{setting_name}%'
            ORDER BY event_time_microseconds DESC
            LIMIT 1
        )
        SELECT count() FROM system.text_log, anchor
        WHERE {recent}
        AND system.text_log.thread_id = anchor.thread_id
        AND level = 'Error'
        AND logger_name = 'TCPHandler'
        AND event_time_microseconds > anchor.t
        AND event_time_microseconds <= (
            SELECT min(event_time_microseconds)
            FROM system.text_log, anchor
            WHERE {recent}
            AND system.text_log.thread_id = anchor.thread_id
            AND message LIKE '%Done processing connection%'
            AND event_time_microseconds > anchor.t
        )
        SETTINGS max_result_rows=0, max_rows_to_read=0
    """)

    if int(error_count) > 0:
        print(f"FAIL: {error_count} extra error(s) — server read from desync'd buffer")
        sys.exit(1)

    print("connection closed after settings parse error")


def test_server_still_alive():
    """Verify the server is still accepting new connections after the bad one."""
    result = clickhouse_query("SELECT 'ok'")
    assert result == "ok", f"Expected 'ok', got: {result}"
    print("server alive after bad settings connection")


def main():
    test_connection_closed_after_bad_settings()
    test_server_still_alive()


if __name__ == "__main__":
    main()
