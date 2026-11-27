import socket
import time

import pytest

from helpers.client import Client
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/config.xml"])

INTROSPECTION_PORT = 9010


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        # `cluster.start` only waits until the port completes a TCP handshake; wait until a query goes through.
        node.query("SELECT 1")
        yield cluster
    finally:
        cluster.shutdown()


def intro():
    return Client(node.ip_address, INTROSPECTION_PORT, command=cluster.client_bin_path)


def wait_metric(name, expected):
    """`system.metrics` read through the introspection port: its own connection counts once in the shared metrics."""
    assert_eq_with_retry(intro(), f"SELECT value FROM system.metrics WHERE metric = '{name}'", f"{expected}\n")


def wait_async_metric(name, expected):
    assert_eq_with_retry(
        intro(), f"SELECT toUInt64(value) FROM system.asynchronous_metrics WHERE metric = '{name}'", f"{expected}\n"
    )


def read_async_metric(name):
    for _ in range(60):
        value = intro().query(f"SELECT toUInt64(value) FROM system.asynchronous_metrics WHERE metric = '{name}'")
        if value:
            return int(value)
        time.sleep(0.5)
    raise AssertionError(f"No asynchronous metric {name}")


def wait_until(predicate):
    for _ in range(60):
        if predicate():
            return
        time.sleep(0.5)
    assert predicate()


def raw_connection(port):
    sock = socket.create_connection((node.ip_address, port), timeout=10)
    sock.setblocking(False)
    return sock


def raw_native():
    """A native connection that sends nothing: its handler waits for the handshake and holds its thread."""
    return raw_connection(9000)


def is_open(sock):
    try:
        return sock.recv(1, socket.MSG_PEEK) != b""
    except BlockingIOError:
        return True
    except ConnectionError:
        return False


def read_until_eof(sock):
    sock.setblocking(True)
    sock.settimeout(30)
    data = b""
    while chunk := sock.recv(4096):
        data += chunk
    return data


def test_connections_wait_for_a_thread(started_cluster):
    wait_metric("TCPServerThreadsActive", 1)
    rejected_tcp = read_async_metric("TCPRejectedConnections")
    rejected_http = read_async_metric("HTTPRejectedConnections")
    refused_lines = node.count_in_log("Refusing connection from")

    sockets = []
    try:
        # `max_connections` = 2: these two take both threads of the main pool. Each one is opened after the previous one
        # got its thread: until then it counts as waiting, and `listen_backlog` = 1 waiting connection is the limit.
        s1 = raw_native()
        sockets.append(s1)
        wait_metric("TCPServerThreadsActive", 2)
        s2 = raw_native()
        sockets.append(s2)
        wait_metric("TCPServerThreadsActive", 3)
        assert is_open(s1) and is_open(s2)

        # A connection to another listener of the same pool waits for a thread.
        h = raw_connection(8123)
        sockets.append(h)
        h.sendall(b"GET /?query=SELECT%201 HTTP/1.0\r\n\r\n")
        wait_metric("TCPServerThreadsScheduled", 4)
        wait_metric("TCPServerThreadsActive", 3)
        assert is_open(h)

        # The native listener had no waiting connection, so this one waits too...
        s3 = raw_native()
        sockets.append(s3)
        wait_metric("TCPServerThreadsScheduled", 5)

        # ...and with `listen_backlog` = 1 connection waiting on the listener, the next one is refused.
        s4 = raw_native()
        sockets.append(s4)
        wait_until(lambda: not is_open(s4))
        wait_until(lambda: int(node.count_in_log("Refusing connection from")) == int(refused_lines) + 1)
        assert node.contains_in_log("reached max queue size 1")
        wait_async_metric("TCPRejectedConnections", rejected_tcp + 1)
        wait_async_metric("HTTPRejectedConnections", rejected_http)

        # A thread becomes free: the HTTP request, waiting first, is served, then the waiting native connection.
        s1.close()
        response = read_until_eof(h)
        assert b" 200 " in response.split(b"\r\n", 1)[0]
        assert response.endswith(b"\r\n\r\n1\n")
        wait_metric("TCPServerThreadsActive", 3)
        wait_metric("TCPServerThreadsScheduled", 3)
        assert is_open(s2) and is_open(s3)

        s2.close()
        s3.close()
        wait_metric("TCPServerThreadsActive", 1)
        wait_metric("TCPServerThreadsScheduled", 1)
        assert int(intro().query("SELECT value FROM system.metrics WHERE metric = 'TCPServerThreads'")) >= 1
    finally:
        for sock in sockets:
            sock.close()

    for _ in range(30):
        assert node.query("SELECT 1") == "1\n"
    assert read_async_metric("TCPRejectedConnections") == rejected_tcp + 1
