import socket
import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # `cluster.start` only waits until the port completes a TCP handshake; wait until a query goes through.
        node.query("SELECT 1")
        yield cluster
    finally:
        cluster.shutdown()


def open_idle_connection():
    """A native connection that sends nothing: its handler waits for the handshake and holds its thread."""
    sock = socket.create_connection((node.ip_address, 9000), timeout=10)
    sock.setblocking(False)
    return sock


def is_open(sock):
    try:
        return sock.recv(1) != b""
    except BlockingIOError:
        return True
    except ConnectionError:
        return False


def test_connection_waits_for_a_thread(started_cluster):
    sockets = []
    try:
        # `max_connections` = 4: these connections take all the threads.
        for _ in range(4):
            sockets.append(open_idle_connection())
            time.sleep(0.2)
        time.sleep(1)
        assert all(is_open(sock) for sock in sockets)

        # The next connection waits for a thread instead of being refused...
        sockets.append(open_idle_connection())
        time.sleep(1)
        assert is_open(sockets[4])

        # ...and with `listen_backlog` = 1 connection already waiting, one more is refused at once.
        start = time.monotonic()
        node.query_and_get_error("SELECT 1", timeout=30)
        assert time.monotonic() - start < 10

        # Two threads become free: one serves the waiting connection, the other the next query.
        sockets[0].close()
        sockets[1].close()
        assert_eq_with_retry(node, "SELECT 1", "1\n")
        assert_eq_with_retry(
            node,
            "SELECT value FROM system.metrics WHERE metric = 'TCPServerThreadsActive'",
            "4\n",
        )
        assert is_open(sockets[4])
    finally:
        for sock in sockets:
            sock.close()

    assert_eq_with_retry(
        node,
        "SELECT value > 0 FROM system.asynchronous_metrics WHERE metric = 'TCPRejectedConnections'",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )
    assert_eq_with_retry(
        node,
        "SELECT value < 4 FROM system.asynchronous_metrics WHERE metric = 'TCPThreads'",
        "1\n",
        retry_count=30,
        sleep_time=1,
    )

    # The threads go back to the pool.
    for _ in range(30):
        assert node.query("SELECT 1") == "1\n"
