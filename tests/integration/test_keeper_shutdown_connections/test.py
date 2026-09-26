import socket
import struct
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/keeper.xml",
        "configs/ssl_conf.xml",
        "configs/dhparam.pem",
        "configs/server.crt",
        "configs/server.key",
    ],
    stay_alive=True,
)

int_struct = struct.Struct("!i")
int_int_long_struct = struct.Struct("!iiq")
int_long_int_long_struct = struct.Struct("!iqiq")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, [node])
        yield cluster
    finally:
        cluster.shutdown()


def open_keeper_session(session_timeout):
    client = keeper_utils.get_keeper_socket(cluster, node.name)
    try:
        request = bytearray()
        request.extend(int_long_int_long_struct.pack(0, 0, session_timeout, 0))
        request.extend(int_struct.pack(16))
        request.extend(b"\x00" * 16)
        request.extend(b"\x00")
        client.sendall(int_struct.pack(45) + request)

        response = client.recv(1000)
        protocol_version, negotiated_timeout, session_id = int_int_long_struct.unpack_from(
            response, 4
        )
        assert protocol_version == 0
        assert session_id != 0
        assert negotiated_timeout == session_timeout
        return client, session_id
    except Exception:
        client.close()
        raise


def wait_for_log_contains(substring, timeout=10):
    deadline = time.monotonic() + timeout
    while not node.contains_in_log(substring):
        assert time.monotonic() < deadline, f"Log line did not appear: {substring}"
        time.sleep(0.1)


def test_idle_connection_does_not_delay_shutdown(started_cluster):
    node.query(
        "CREATE TABLE replicated_table (value UInt64) "
        "ENGINE=ReplicatedMergeTree('/clickhouse/tables/replicated_table', 'node') "
        "ORDER BY tuple()"
    )
    node.query("INSERT INTO replicated_table VALUES (1)")

    session_timeout_ms = 60_000
    graceful_shutdown_deadline_seconds = 30
    assert graceful_shutdown_deadline_seconds * 1000 < session_timeout_ms

    client, _ = open_keeper_session(session_timeout=session_timeout_ms)
    clickhouse_pid = node.get_process_pid("clickhouse server")
    assert clickhouse_pid is not None
    node.exec_in_container(
        ["bash", "-c", f"kill -TERM {clickhouse_pid}"], user="root"
    )

    try:
        node.wait_start_failed(graceful_shutdown_deadline_seconds)

        client.settimeout(3)
        assert client.recv(1) == b""
        assert node.contains_in_log(
            "Will not wait for unique parts to be fetched because we don't have any unique parts"
        )
    finally:
        client.close()
        if node.get_process_pid("clickhouse server") is not None:
            node.stop_clickhouse(kill=True)
        node.start_clickhouse()


def test_http_control_request_finishes_before_keeper_shutdown(started_cluster):
    node.query("DROP TABLE IF EXISTS replicated_table SYNC")
    node.restart_clickhouse()
    keeper_utils.wait_nodes(cluster, [node])

    idle_client, idle_session_id = open_keeper_session(session_timeout=60_000)
    client = socket.create_connection((node.ip_address, 9182), timeout=5)
    client.settimeout(10)
    request_body = b"value"
    request = (
        b"POST /api/v1/storage/shutdown_http_request HTTP/1.1\r\n"
        b"Host: node\r\n"
        b"Connection: close\r\n"
        b"Expect: 100-continue\r\n"
        + f"Content-Length: {len(request_body)}\r\n\r\n".encode()
    )
    client.sendall(request + request_body[:-1])
    assert client.recv(4096).startswith(b"HTTP/1.1 100 Continue")

    clickhouse_pid = node.get_process_pid("clickhouse server")
    assert clickhouse_pid is not None
    node.exec_in_container(
        ["bash", "-c", f"kill -TERM {clickhouse_pid}"], user="root"
    )

    try:
        wait_for_log_contains(
            "Closed all non-Keeper-TCP listening sockets. Waiting for 1 outstanding connections."
        )

        client.sendall(request_body[-1:])
        response = bytearray()
        while data := client.recv(4096):
            response.extend(data)
        assert response.startswith(b"HTTP/1.1 201 Created")

        wait_for_log_contains(
            f"Keeper TCP drain started, closing session #{idle_session_id}"
        )
        wait_for_log_contains("Trying to close ")
        node.wait_start_failed(30)
    finally:
        client.close()
        idle_client.close()
        if node.get_process_pid("clickhouse server") is not None:
            node.stop_clickhouse(kill=True)
        node.start_clickhouse()


# A handler stalled in a read is released only when the socket times out, which is bounded by
# max(receive_timeout, send_timeout) of the global profile settings; both default to 300s.
STALLED_READ_BOUND_SEC = 300

# One handler parked in the read is enough for the sweep; several independent connections damp
# per-thread scheduling jitter. They do not help if the whole host stalls.
STALLED_CONNECTIONS = 4

# Logged when a handler's first read reports EOF, which here means the shutdown sweep closed the
# socket under it. The logger name is part of the marker: TCPHandler emits the same sentence for
# the native protocol port.
WAKE_MARKER = "KeeperTCPHandler: Client has not sent any data"


@pytest.mark.parametrize("port", [9181, 9281], ids=["plain", "secure"])
def test_connection_stalled_before_handshake_does_not_delay_shutdown(
    started_cluster, port
):
    graceful_shutdown_deadline_seconds = 30
    assert graceful_shutdown_deadline_seconds < STALLED_READ_BOUND_SEC

    node.query("DROP TABLE IF EXISTS replicated_table SYNC")
    keeper_utils.wait_nodes(cluster, [node])

    # Bare TCP connects that never send a TLS ClientHello: on the secure listener each handler
    # parks inside the handshake read, which is where it holds the socket's SSL mutex.
    clients = [
        keeper_utils.get_keeper_socket(cluster, node.name, port=port)
        for _ in range(STALLED_CONNECTIONS)
    ]
    try:
        # A later connection that is accepted and answered: any shutdown delay measured below is
        # then not the stalled connections merely sitting in the accept queue.
        assert keeper_utils.send_4lw_cmd(cluster, node, "ruok") == "imok"

        # Nothing is logged between accept and that first read, so the handlers are given a moment
        # to reach it; the marker assertion below is what proves they did.
        time.sleep(5)
        marker_count_before = int(node.count_in_log(WAKE_MARKER))

        clickhouse_pid = node.get_process_pid("clickhouse server")
        assert clickhouse_pid is not None
        node.exec_in_container(
            ["bash", "-c", f"kill -TERM {clickhouse_pid}"], user="root"
        )
        node.wait_start_failed(graceful_shutdown_deadline_seconds)

        # A count delta, not contains_in_log: the log accumulates across this module's restarts,
        # and the plain parametrization runs first and writes the very same line.
        assert int(node.count_in_log(WAKE_MARKER)) > marker_count_before, (
            "no stalled handler was woken out of its first read, so this run did not exercise "
            "the stall the test exists for"
        )
    finally:
        for client in clients:
            client.close()
        if node.get_process_pid("clickhouse server") is not None:
            node.stop_clickhouse(kill=True)
        node.start_clickhouse()
