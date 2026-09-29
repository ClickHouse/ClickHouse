import select
import socket
import struct
import time

import psycopg2

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/handshake_timeout.xml",
        "configs/ssl_conf.xml",
        "configs/server.crt",
        "configs/server.key",
        "configs/dhparam.pem",
    ],
    user_configs=["configs/pg_user.xml"],
)

# A node whose `receive_timeout` is well below the handshake budget: the phase must obey the
# smaller of the two, including on the reads that bypass the buffer.
short_timeout_node = cluster.add_instance(
    "short_timeout_node",
    main_configs=["configs/short_receive_timeout.xml"],
    user_configs=["configs/short_receive_timeout.xml"],
)

# The raw pre-SSL loop reads at most 36 bytes, so outlasting a budget with it needs a small one:
# bytes have to arrive faster than the floor on the read window, or the read times out first.
fast_deadline_node = cluster.add_instance(
    "fast_deadline_node",
    main_configs=["configs/fast_deadline.xml"],
)
FAST_DEADLINE = 1

MYSQL_PORT = 9001
POSTGRESQL_PORT = 9005
SHORT_RECEIVE_TIMEOUT = 1
SHORT_NODE_HANDSHAKE_TIMEOUT = 9
# `handshake_timeout_milliseconds` from the config, in seconds.
HANDSHAKE_TIMEOUT = 3
# The server has to close within the budget plus slack. A client-side timeout is a failure: it would
# also happen if the server sat on `receive_timeout`, 300 s, which is the bug under test.
DISCONNECT_DEADLINE = 4 * HANDSHAKE_TIMEOUT
# Has to stay under the floor the deadline keeps on the read window (100 ms), so every byte lands
# while a read is waiting and the deadline is what cuts the connection, not the socket timeout.
TRICKLE_INTERVAL = 0.05
TRICKLE_STEPS = int(3 * HANDSHAKE_TIMEOUT / TRICKLE_INTERVAL)
# The TLS case needs to outlast the budget while feeding a byte per interval.
TLS_TRICKLE_STEPS = int(2 * HANDSHAKE_TIMEOUT / TRICKLE_INTERVAL)

CLIENT_PROTOCOL_41 = 0x00000200
CLIENT_SSL = 0x00000800

# Both silence cases log this, so each one waits for one more than the log already holds.
SOCKET_TIMEOUT_LINE = "Timeout exceeded while reading from socket"
HANDSHAKE_TIMEOUT_LINE = "Handshake timeout exceeded"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def connect_and_read_greeting(instance=None):
    """Open a MySQL connection and consume the server handshake packet."""
    instance = instance or node
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(DISCONNECT_DEADLINE)
    sock.connect((instance.ip_address, MYSQL_PORT))
    greeting = sock.recv(4096)
    # 3-byte payload length, 1-byte sequence id 0, then protocol version 10.
    assert len(greeting) > 5, f"No MySQL handshake packet: {greeting!r}"
    assert greeting[3] == 0, f"Unexpected sequence id: {greeting!r}"
    assert greeting[4] == 10, f"Unexpected protocol version: {greeting!r}"
    return sock


def disconnected(sock):
    """Whether the server has hung up, without blocking. A reset counts: the server closes while the
    bytes we trickled are still unread, and Linux answers that with RST rather than FIN."""
    readable, _, _ = select.select([sock], [], [], 0)
    if not readable:
        return False
    try:
        return not sock.recv(4096)
    except OSError:
        return True


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


def test_trickled_first_bytes_are_disconnected(started_cluster):
    """Trickling the pre-SSL bytes must be cut at the budget too.

    They are read straight off the socket, so the deadline has to be enforced there and not only
    re-applied: re-arming a floor-sized window per byte would outlast the budget.
    """
    seen = int(fast_deadline_node.count_in_log(HANDSHAKE_TIMEOUT_LINE))
    sock = connect_and_read_greeting(fast_deadline_node)
    started = time.monotonic()
    try:
        # A 36-byte SSLRequest, one byte at a time, so every read re-enters the raw loop. At 50 ms a
        # byte it outlasts the 1 s budget while staying inside the 100 ms read window.
        payload = struct.pack("<IIB", CLIENT_PROTOCOL_41 | CLIENT_SSL, 16777216, 45) + b"\x00" * 23
        for byte in struct.pack("<I", len(payload) | (1 << 24)) + payload:
            time.sleep(TRICKLE_INTERVAL)
            if disconnected(sock):
                break
            try:
                sock.sendall(bytes([byte]))
            except OSError:
                break
        else:
            wait_for_disconnect(sock)

        elapsed = time.monotonic() - started
        assert elapsed < 4 * FAST_DEADLINE, f"Raw handshake reads held for {elapsed} seconds"
    finally:
        sock.close()

    fast_deadline_node.wait_for_log_line(HANDSHAKE_TIMEOUT_LINE, repetitions=seen + 1)


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


def test_trickled_tls_handshake_is_disconnected(started_cluster):
    """A client that dribbles its TLS ClientHello must be cut off at the budget.

    OpenSSL reads inside its own state machine, so on a blocking socket only `SO_RCVTIMEO` applied,
    per read, and a byte before each timeout kept the negotiation alive indefinitely.
    """
    seen = int(node.count_in_log(SOCKET_TIMEOUT_LINE))
    sock = connect_and_read_greeting()
    started = time.monotonic()
    try:
        # SSLRequest, which makes the server upgrade, then a TLS record header claiming 512 bytes.
        payload = struct.pack("<IIB", CLIENT_PROTOCOL_41 | CLIENT_SSL, 16777216, 45) + b"\x00" * 23
        sock.sendall(struct.pack("<I", len(payload) | (1 << 24)) + payload)
        sock.sendall(bytes([0x16, 0x03, 0x01, 0x02, 0x00]))

        for _ in range(TLS_TRICKLE_STEPS):
            time.sleep(TRICKLE_INTERVAL)
            if disconnected(sock):
                break
            try:
                sock.sendall(b"\x00")
            except OSError:
                break
        else:
            wait_for_disconnect(sock)

        elapsed = time.monotonic() - started
        assert elapsed < DISCONNECT_DEADLINE, f"TLS negotiation held for {elapsed} seconds"
    finally:
        sock.close()

    node.wait_for_log_line(SOCKET_TIMEOUT_LINE, repetitions=seen + 1)


def test_receive_timeout_is_not_widened_by_the_deadline(started_cluster):
    """`receive_timeout` below the handshake budget stays the shorter of the two.

    The raw pre-SSL reads re-apply the deadline to the socket, so they must take the minimum with
    what the socket was configured with rather than writing the remaining budget over it.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(SHORT_NODE_HANDSHAKE_TIMEOUT)
    try:
        sock.connect((short_timeout_node.ip_address, MYSQL_PORT))
        greeting = sock.recv(4096)
        assert len(greeting) > 5, f"No MySQL handshake packet: {greeting!r}"

        started = time.monotonic()
        wait_for_disconnect(sock)
        elapsed = time.monotonic() - started
        assert elapsed < SHORT_NODE_HANDSHAKE_TIMEOUT / 2, (
            f"Held for {elapsed} seconds, `receive_timeout` is {SHORT_RECEIVE_TIMEOUT}"
        )
    finally:
        sock.close()


def test_silent_postgresql_client_is_disconnected(started_cluster):
    """The PostgreSQL listener leaves the socket without a receive timeout, so the deadline is the
    only thing that bounds a client which connects and then says nothing."""
    seen = int(node.count_in_log(SOCKET_TIMEOUT_LINE))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(DISCONNECT_DEADLINE)
    try:
        sock.connect((node.ip_address, POSTGRESQL_PORT))
        # PostgreSQL has the client speak first, so sending nothing is the whole test.
        elapsed = wait_for_disconnect(sock)
        assert elapsed >= HANDSHAKE_TIMEOUT - 2, f"Disconnected after {elapsed} seconds, too early"
    finally:
        sock.close()

    node.wait_for_log_line(SOCKET_TIMEOUT_LINE, repetitions=seen + 1)


def test_postgresql_session_outlives_the_handshake_budget(started_cluster):
    """Clearing the deadline has to restore what the socket had before it, not the clamp.

    The PostgreSQL listener leaves the socket without a receive timeout. The TLS upgrade replaces the
    buffer while the socket is clamped, so a baseline read off that socket would outlive the handshake
    and start cutting idle sessions.
    """
    connection = psycopg2.connect(
        host=node.ip_address,
        port=POSTGRESQL_PORT,
        user="pg_user",
        password="123",
        database="default",
        sslmode="require",
    )
    try:
        # Idle well past the budget, then prove the session still works.
        time.sleep(2 * HANDSHAKE_TIMEOUT)
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchall() == [(1,)]
    finally:
        connection.close()


def test_server_healthy_after_disconnects(started_cluster):
    assert node.query("SELECT 1").strip() == "1"
