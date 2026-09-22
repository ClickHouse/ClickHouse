import socket
import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/handshake_timeout.xml"],
)

MYSQL_PORT = 9001
# `handshake_timeout_milliseconds` from the config, in seconds.
HANDSHAKE_TIMEOUT = 3
# The server has to close within the budget plus slack. A client-side timeout is a failure: it would
# also happen if the server sat on `receive_timeout`, 300 s, which is the bug under test.
DISCONNECT_DEADLINE = 4 * HANDSHAKE_TIMEOUT
# Under the 500 ms floor the deadline keeps on the read window, so every byte lands while a read is
# waiting and the deadline is what cuts the connection. Together the steps outlast the budget.
TRICKLE_INTERVAL = 0.2
TRICKLE_STEPS = 40

# Both silence cases log this, so each one waits for one more than the log already holds.
SOCKET_TIMEOUT_LINE = "Timeout exceeded while reading from socket"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def connect_and_read_greeting():
    """Open a MySQL connection and consume the server handshake packet."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(DISCONNECT_DEADLINE)
    sock.connect((node.ip_address, MYSQL_PORT))
    greeting = sock.recv(4096)
    # 3-byte payload length, 1-byte sequence id 0, then protocol version 10.
    assert len(greeting) > 5, f"No MySQL handshake packet: {greeting!r}"
    assert greeting[3] == 0, f"Unexpected sequence id: {greeting!r}"
    assert greeting[4] == 10, f"Unexpected protocol version: {greeting!r}"
    return sock


def wait_for_disconnect(sock):
    """Wait until the server hangs up, and return how long that took.

    A client-side timeout fails the test: only the server closing proves the bound.
    """
    started = time.monotonic()
    while True:
        try:
            if not sock.recv(4096):
                return time.monotonic() - started
        except socket.timeout:
            raise AssertionError(
                f"Server kept the connection for more than {DISCONNECT_DEADLINE} seconds"
            )
        except (ConnectionResetError, BrokenPipeError, OSError):
            return time.monotonic() - started


def test_trickled_handshake_is_disconnected(started_cluster):
    """A trickling client must be cut off by the wall-clock deadline.

    The pace has to stay under the floor on the read window, or the read times out first.
    """
    sock = connect_and_read_greeting()
    try:
        # A packet header declaring a 64-byte payload, then trickle the payload.
        sock.sendall(bytes([64, 0, 0, 1]))
        for _ in range(TRICKLE_STEPS):
            time.sleep(TRICKLE_INTERVAL)
            try:
                sock.sendall(b"A")
            except (BrokenPipeError, ConnectionResetError, OSError):
                break  # The server hung up while we were trickling - expected.
        else:
            # Writes to a closed connection do not always fail, so confirm the hangup by reading.
            elapsed = wait_for_disconnect(sock)
            assert elapsed < DISCONNECT_DEADLINE
    finally:
        sock.close()

    # Name the mechanism, so that losing the deadline cannot pass as a disconnect for another reason.
    node.wait_for_log_line("Handshake timeout exceeded")


def test_silence_after_packet_header_is_disconnected(started_cluster):
    """A client that promises a payload and then goes quiet must be cut off by the receive timeout."""
    seen = int(node.count_in_log(SOCKET_TIMEOUT_LINE))
    sock = connect_and_read_greeting()
    try:
        # Declare a 16 KiB payload and send none of it.
        sock.sendall(bytes([0x00, 0x40, 0x00, 1]))
        elapsed = wait_for_disconnect(sock)
        assert elapsed >= HANDSHAKE_TIMEOUT - 2, f"Disconnected after {elapsed} seconds, too early to be the timeout"
    finally:
        sock.close()

    node.wait_for_log_line(SOCKET_TIMEOUT_LINE, repetitions=seen + 1)


def test_silence_before_any_bytes_is_disconnected(started_cluster):
    """A client that takes the greeting and sends nothing at all must be cut off.

    `finishHandshake` reads its first bytes with raw `socket().receiveBytes`, which never reaches the
    read buffer, so only the timeout armed on the socket itself bounds this.
    """
    seen = int(node.count_in_log(SOCKET_TIMEOUT_LINE))
    sock = connect_and_read_greeting()
    try:
        elapsed = wait_for_disconnect(sock)
        assert elapsed >= HANDSHAKE_TIMEOUT - 2, f"Disconnected after {elapsed} seconds, too early"
    finally:
        sock.close()

    node.wait_for_log_line(SOCKET_TIMEOUT_LINE, repetitions=seen + 1)


def test_server_healthy_after_disconnects(started_cluster):
    assert node.query("SELECT 1").strip() == "1"
