#!/usr/bin/env python3
# Checks that a Raft vote request from a server that is not part of the Keeper
# cluster configuration cannot advance (and persist) a node's term. A single
# crafted request-vote frame with an unconfigured source id and term UINT64_MAX,
# sent to each Raft listener, must not leave the cluster without a leader or wedge
# it across a restart.
import socket
import struct
import zlib

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1", main_configs=["configs/enable_keeper1.xml"], stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/enable_keeper2.xml"], stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/enable_keeper3.xml"], stay_alive=True
)
nodes = [node1, node2, node3]

RAFT_PORT = 9234
REQUEST_VOTE_REQUEST = 1
UINT64_MAX = 0xFFFFFFFFFFFFFFFF


def make_vote_frame(src, dst, term):
    # NuRaft request header, little-endian, 54 bytes:
    # marker(1) type(1) src(4) dst(4) term(8) last_log_term(8) last_log_idx(8)
    # commit_idx(8) data_size(4), then flags(4)+CRC32(4) over the first 46 bytes.
    header = struct.pack(
        "<BBiiQQQQi",
        0x0,
        REQUEST_VOTE_REQUEST,
        src,
        dst,
        term,
        term,
        UINT64_MAX,
        0,
        0,
    )
    crc = zlib.crc32(header, 0) & 0xFFFFFFFF
    return header + struct.pack("<Q", crc)


def send_vote_frame(nodename, src, dst, term):
    host = cluster.get_instance_ip(nodename)
    s = socket.socket()
    s.settimeout(10)
    try:
        s.connect((host, RAFT_PORT))
        s.sendall(make_vote_frame(src, dst, term))
        try:
            s.recv(64)
        except socket.timeout:
            pass
    finally:
        s.close()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, nodes)
        yield cluster
    finally:
        cluster.shutdown()


def create_on_leader(path):
    leader = keeper_utils.get_leader(cluster, nodes)
    zk = keeper_utils.get_fake_zk(cluster, leader.name)
    try:
        zk.create(path, b"ok")
        assert zk.get(path)[0] == b"ok"
    finally:
        zk.stop()
        zk.close()


def test_unconfigured_vote_sender_cannot_poison_term(started_cluster):
    # Baseline: healthy cluster with a leader and a working write.
    keeper_utils.wait_nodes(cluster, nodes)
    create_on_leader("/before_attack")

    # Attack: one unconfigured, max-term vote frame to each Raft listener.
    for node_id, node in enumerate(nodes, start=1):
        send_vote_frame(node.name, src=999, dst=node_id, term=UINT64_MAX)

    # Each node must have reached the rejection gate: proves the frame was
    # actually dispatched (and not silently dropped at the socket layer).
    for node in nodes:
        assert node.contains_in_log(
            "received a vote request from unconfigured peer 999"
        ), f"{node.name} did not log the vote-request rejection"

    # The cluster must keep a working quorum: a leader and a new write.
    keeper_utils.wait_nodes(cluster, nodes)
    create_on_leader("/after_attack")

    # And it must survive a full same-state restart: stop every node, then start
    # all of them before waiting for readiness (a single node blocks on quorum),
    # so a persisted poisoned term would be reloaded from disk with no live quorum
    # to paper over it.
    for node in nodes:
        node.stop_clickhouse()
    for node in nodes:
        node.start_clickhouse(wait_start=False)
    keeper_utils.wait_nodes(cluster, nodes)
    create_on_leader("/after_restart")

    leader = keeper_utils.get_leader(cluster, nodes)
    zk = keeper_utils.get_fake_zk(cluster, leader.name)
    try:
        assert zk.get("/before_attack")[0] == b"ok"
    finally:
        zk.stop()
        zk.close()
