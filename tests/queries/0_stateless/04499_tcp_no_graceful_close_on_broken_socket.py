#!/usr/bin/env python3
"""Test connection termination after an exception in the TCP handler.

Two complementary behaviors:

1. A query error whose code merely looks network-related (`SOCKET_TIMEOUT`,
   `NETWORK_ERROR` — e.g. from `url()` or `remote()` reads) must still be
   delivered to the client as an Exception packet, and the connection must
   stay usable for further queries.

2. When reading from the client socket itself fails (here: the client resets
   the connection mid-query), the server must close the connection right away
   without attempting graceful termination — no writes of logs or the
   exception into the broken socket.
"""

import os
import socket
import struct
import subprocess
import sys
import time
import uuid

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


def send_query(sock, query_text, query_id=""):
    pkt = bytearray()
    pkt += write_varuint(1)
    pkt += write_string(query_id)
    pkt += build_client_info()
    pkt += write_string("")  # Empty settings block
    pkt += write_varuint(2)  # Stage: Complete
    pkt += write_varuint(0)  # No compression
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
    read_string(sock)
    message = read_string(sock)
    _stack_trace = read_string(sock)
    has_nested = recv_exact(sock, 1)[0]
    if has_nested:
        read_exception(sock)
    return code, message


def clickhouse_query(query):
    cmd = CLICKHOUSE_CLIENT.split() + ["--query", query]
    return subprocess.run(
        cmd, capture_output=True, text=True, check=True
    ).stdout.strip()


# -- Tests --------------------------------------------------------------------


def read_progress(sock):
    """Consume a Progress packet (after packet type is already read).

    At revision 54440: read_rows, read_bytes, total_rows_to_read,
    written_rows, written_bytes.
    """
    for _ in range(5):
        read_varuint(sock)


def test_query_error_with_network_code_is_delivered():
    """A query failure with a network-looking error code is not a broken
    connection: the client must receive the Exception packet with the original
    code, and the connection must remain usable.

    The error is simulated with throwIf and a custom error code. The query is
    an INSERT SELECT, so no result header precedes the failure: the response
    can only contain Progress packets before the Exception.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)
    sock.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT))

    try:
        send_hello(sock)
        recv_hello(sock)

        for expected_code in (209, 210):  # SOCKET_TIMEOUT, NETWORK_ERROR
            send_query(
                sock,
                "INSERT INTO FUNCTION null('x UInt8')"
                f" SELECT throwIf(1, 'simulated network-looking error', toInt16({expected_code}))"
                " SETTINGS allow_custom_error_code_in_throwif = 1",
            )
            send_empty_block(sock)

            while True:
                pkt_type = read_varuint(sock)
                if pkt_type == 3:  # Progress
                    read_progress(sock)
                    continue
                assert pkt_type == 2, f"Expected Exception packet (2), got {pkt_type}"
                break
            code, message = read_exception(sock)
            assert (
                code == expected_code
            ), f"Expected code {expected_code}, got {code}: {message}"

        # The connection must be preserved after the exceptions.
        sock.sendall(write_varuint(4))  # Ping
        pkt_type = read_varuint(sock)
        assert pkt_type == 4, f"Expected Pong (4), got {pkt_type}"
    finally:
        sock.close()

    print("query error with network error code is delivered, connection preserved")


def test_no_graceful_termination_on_broken_socket():
    """When the server's read from the client socket fails, the server must
    skip graceful termination (no attempts to send logs/exception into the
    broken socket) and close the connection.

    The client resets the connection (RST) while the server waits for external
    table data, so the server's read fails with a network error. Verified via
    system.text_log on the handler thread: the fast-close message is present
    and no graceful-termination failures ("Can't send ...") are logged. The
    exception itself is still logged at Error level — that one is expected.
    """
    query_id = str(uuid.uuid4())

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(30)
    sock.connect((CLICKHOUSE_HOST, CLICKHOUSE_PORT))

    try:
        send_hello(sock)
        recv_hello(sock)

        # No empty block is sent: the server keeps waiting for external
        # table data, so the query stays running until the reset below.
        send_query(sock, "SELECT 1", query_id=query_id)

        # Wait until the server has started processing the query.
        for _ in range(600):
            if (
                clickhouse_query(
                    f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'"
                )
                == "1"
            ):
                break
            time.sleep(0.05)
        else:
            print(f"FAIL: query {query_id} did not appear in system.processes")
            sys.exit(1)
    finally:
        # Reset the connection: the server's read fails with a network error.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        sock.close()

    recent = "event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE"

    # Wait until the server has finished tearing down the connection.
    for _ in range(600):
        clickhouse_query("SYSTEM FLUSH LOGS text_log")
        done = clickhouse_query(f"""
            WITH anchor AS (
                SELECT thread_id, event_time_microseconds AS t
                FROM system.text_log
                WHERE {recent}
                AND query_id = '{query_id}'
                ORDER BY event_time_microseconds
                LIMIT 1
            )
            SELECT count() FROM system.text_log, anchor
            WHERE {recent}
            AND system.text_log.thread_id = anchor.thread_id
            AND logger_name = 'TCPHandler'
            AND message LIKE '%Done processing connection%'
            AND event_time_microseconds > anchor.t
            SETTINGS max_result_rows = 0, max_rows_to_read = 0
        """)
        if done != "0":
            break
        time.sleep(0.05)
    else:
        print(f"FAIL: no connection teardown found in text_log for {query_id}")
        sys.exit(1)

    # The counted messages are logged while the thread is still attached to
    # the query, so they carry the query_id (unlike the teardown logs above).
    counts = clickhouse_query(f"""
        SELECT
            countIf(message LIKE '%Going to close connection without graceful termination%'),
            countIf(message LIKE 'Can''t send logs to client%'
                 OR message LIKE 'Can''t send exception to client%'
                 OR message LIKE 'Can''t skip excessive input packets%')
        FROM system.text_log
        WHERE {recent}
        AND query_id = '{query_id}'
        AND logger_name = 'TCPHandler'
        SETTINGS max_result_rows = 0, max_rows_to_read = 0
    """)
    fast_close_count, send_failure_count = counts.split("\t")

    if fast_close_count == "0":
        print("FAIL: connection was not closed via the no-graceful-termination path")
        sys.exit(1)
    if send_failure_count != "0":
        print(
            f"FAIL: {send_failure_count} send failure(s) — server tried to use the broken socket"
        )
        sys.exit(1)

    print("no graceful termination on broken client socket")


def main():
    test_query_error_with_network_code_is_delivered()
    test_no_graceful_termination_on_broken_socket()


if __name__ == "__main__":
    main()
