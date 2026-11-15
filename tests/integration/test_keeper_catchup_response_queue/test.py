#!/usr/bin/env python3

import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import get_retry_number


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/enable_keeper1.xml"],
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/enable_keeper2.xml"],
    stay_alive=True,
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/enable_keeper3.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, [node1, node2, node3])
        yield cluster
    finally:
        cluster.shutdown()


def parse_4lw(data):
    result = {}
    for line in data.splitlines():
        if "\t" not in line:
            continue
        key, value = line.split("\t", 1)
        result[key] = value
    return result


def get_last_committed_log_idx(node):
    data = keeper_utils.send_4lw_cmd(cluster, node, "lgif")
    return int(parse_4lw(data)["last_committed_log_idx"])


def wait_last_committed_at_least(node, expected, timeout=30.0):
    start = time.time()
    while time.time() - start < timeout:
        if get_last_committed_log_idx(node) >= expected:
            return
        time.sleep(0.1)
    raise AssertionError(f"{node.name} did not catch up to {expected}")


def get_keeper_commits_failed(node):
    return int(
        node.query(
            "SELECT ifNull(any(value), 0) FROM system.events WHERE event = 'KeeperCommitsFailed'"
        ).strip()
    )


def test_catchup_responses_are_drained_before_request_dispatch_start(started_cluster, request):
    retry = get_retry_number(request)
    nodes = [node1, node2, node3]
    leader = keeper_utils.get_leader(cluster, nodes)
    lagger = next(node for node in nodes if node != leader)

    lagger.rotate_logs()
    lagger.stop_clickhouse(kill=True)

    zk = keeper_utils.get_fake_zk(cluster, leader.name, timeout=30.0)
    root = f"/catchup_response_queue_{retry}"
    try:
        zk.create(root, b"")
        payload = b"x" * 128
        for i in range(500):
            zk.create(f"{root}/node_{i:04d}", payload)

        expected_log_idx = get_last_committed_log_idx(leader)
    finally:
        zk.stop()
        zk.close()

    lagger.start_clickhouse(start_wait_sec=120)
    keeper_utils.wait_until_connected(cluster, lagger, timeout=60)
    wait_last_committed_at_least(lagger, expected_log_idx, timeout=30)

    assert get_keeper_commits_failed(lagger) == 0

    matches = lagger.grep_in_log(
        "Response queue is too big for too long",
        only_latest=True,
    ).strip()
    assert matches == "", f"unexpected response queue drop log lines: {matches[:500]}"

    lagger_zk = keeper_utils.get_fake_zk(cluster, lagger.name, timeout=30.0)
    try:
        lagger_zk.create(f"{root}/after_restart", b"ok")
        assert lagger_zk.get(f"{root}/after_restart")[0] == b"ok"
    finally:
        lagger_zk.stop()
        lagger_zk.close()
