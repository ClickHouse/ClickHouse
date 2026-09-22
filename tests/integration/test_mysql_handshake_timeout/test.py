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
# Generous upper bound: the point is that the server does not wait `receive_timeout`, 300 s.
DISCONNECT_DEADLINE = 60
# Under the 500 ms floor the deadline keeps on the read window, so every byte lands while a read is
# waiting and the deadline is what cuts the connection. Together the steps outlast the budget.
TRICKLE_INTERVAL = 0.2
TRICKLE_STEPS = 40


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def connect_and_read_greeting():
    """Open a MySQL connection and consume the server handshake packet.

    Reading the greeting proves the handler runs, so a test waiting for a disconnect cannot pass by
    the server refusing connections outright.
    """
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
    """Wait until the server hangs up, and return how long that took."""
    started = time.monotonic()
    while time.monotonic() - started < DISCONNECT_DEADLINE:
        try:
            if not sock.recv(4096):
                return time.monotonic() - started
        except (ConnectionResetError, BrokenPipeError, socket.timeout, OSError):
            return time.monotonic() - started
    raise AssertionError(f"Server kept the connection for more than {DISCONNECT_DEADLINE} seconds")


def test_trickled_handshake_is_disconnected(started_cluster):
    """A client that trickles its handshake response must be cut off by the wall-clock deadline.

    A pace slower than the floor on the read window would time out the read instead, which bounds
    the connection just as well but reports the socket timeout rather than the deadline.
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
            wait_for_disconnect(sock)
    finally:
        sock.close()

    # Name the mechanism, so that losing the deadline cannot pass as a disconnect for another reason.
    node.wait_for_log_line("Handshake timeout exceeded")


def test_silence_after_packet_header_is_disconnected(started_cluster):
    """A client that promises a payload and then goes quiet must be cut off too.

    No read completes, so the deadline check never runs and the clamped receive timeout is what
    fires. Without it the server waits `receive_timeout`, 300 s.
    """
    sock = connect_and_read_greeting()
    try:
        # Declare a 16 KiB payload and send none of it.
        sock.sendall(bytes([0x00, 0x40, 0x00, 1]))
        elapsed = wait_for_disconnect(sock)
        assert elapsed >= HANDSHAKE_TIMEOUT - 2, f"Disconnected after {elapsed} seconds, too early to be the timeout"
    finally:
        sock.close()

    # The shortened socket receive timeout, not the wall-clock deadline: no read ever completed.
    node.wait_for_log_line("Timeout exceeded while reading from socket")


def test_server_healthy_after_disconnects(started_cluster):
    assert node.query("SELECT 1").strip() == "1"
