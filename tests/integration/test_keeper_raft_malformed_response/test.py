#!/usr/bin/env python3
# Checks that Keeper does not crash when a Raft peer replies with a malformed
# response. A crafted join_cluster_response whose declared hint length exceeds
# the response body used to underflow the context length and overwrite memory.
import socket
import struct
import zlib

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper1.xml"], stay_alive=True
)

RAFT_PORT = 9234
ADD_SERVER_REQUEST = 6
JOIN_CLUSTER_RESPONSE = 13
CLUSTER_SERVER = 3
INCLUDE_HINT = 0x2


def crc32(data):
    return zlib.crc32(data, 0) & 0xFFFFFFFF


def add_server_request(src, dst, new_id, endpoint):
    srv = (
        struct.pack("<i", new_id) + struct.pack("<i", 0)
        + endpoint.encode() + b"\x00" + b"\x00"
        + struct.pack("<B", 0) + struct.pack("<i", 1)
    )
    # log entry: term(u64) val_type(u8=cluster_server) size(i32) data
    le = struct.pack("<Q", 0) + struct.pack("<B", CLUSTER_SERVER) + struct.pack("<i", len(srv)) + srv
    hdr = struct.pack("<BBiiQQQQi", 0x0, ADD_SERVER_REQUEST, src, dst, 0, 0, 0, 0, len(le))
    return hdr + struct.pack("<Q", crc32(hdr)) + le


def join_cluster_response(src, dst, hint_len, pad=4096):
    carried = struct.pack("<HH", 0, hint_len) + struct.pack("<q", 1) + (b"\x41" * pad)
    hdr = struct.pack("<BBiiQQBi", 0x1, JOIN_CLUSTER_RESPONSE, src, dst, 0, 0, 1, len(carried))
    return hdr + struct.pack("<Q", (INCLUDE_HINT << 32) | crc32(hdr)) + carried


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_until_connected(cluster, node1)
        yield cluster
    finally:
        cluster.shutdown()


def send_malformed_response(keeper_ip, hint_len):
    # Determine the address Keeper will use to reach us, and listen on it.
    probe = socket.create_connection((keeper_ip, 9181), timeout=10)
    local_ip = probe.getsockname()[0]
    probe.close()

    listener = socket.socket(); listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((local_ip, 0)); listener.listen(1); listener.settimeout(30)
    local_port = listener.getsockname()[1]

    # Raw add_server_request naming us as server.2 so Keeper connects back.
    s = socket.create_connection((keeper_ip, RAFT_PORT), timeout=10)
    s.sendall(add_server_request(src=2, dst=1, new_id=2, endpoint=f"{local_ip}:{local_port}"))
    try:
        s.recv(256)
    except Exception:
        pass
    s.close()

    # Accept Keeper's join_cluster_request callback and reply with the crafted response.
    conn, _ = listener.accept()
    conn.settimeout(10)
    try:
        conn.recv(4096)
    except Exception:
        pass
    conn.sendall(join_cluster_response(src=2, dst=1, hint_len=hint_len))
    conn.close()
    listener.close()


def test_malformed_hint_length_does_not_crash_keeper(started_cluster):
    keeper_ip = cluster.get_instance_ip(node1.name)

    # hint_len (4105) exceeds the response body -> context length underflow.
    send_malformed_response(keeper_ip, hint_len=4105)

    # The leader must reject the response (not crash): it logs the rejection,
    # which also proves the parser actually processed the crafted response. The
    # parser runs asynchronously, so wait for the line rather than checking once.
    node1.wait_for_log_line(
        "received a malformed response from peer 2", timeout=30
    )

    # And it must stay up and keep serving.
    keeper_utils.wait_until_connected(cluster, node1)
    zk = keeper_utils.get_fake_zk(cluster, node1.name)
    try:
        zk.create("/after_malformed_response", b"ok")
        assert zk.get("/after_malformed_response")[0] == b"ok"
    finally:
        zk.stop()
        zk.close()
