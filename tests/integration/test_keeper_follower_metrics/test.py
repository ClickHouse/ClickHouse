#!/usr/bin/env python3

import csv
import time

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

ALL_NODES = [node1, node2, node3]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        keeper_utils.wait_nodes(cluster, ALL_NODES)
        yield cluster
    finally:
        cluster.shutdown()


def parse_mntr(data):
    result = {}
    reader = csv.reader(data.split("\n"), delimiter="\t")
    for row in reader:
        if len(row) == 2:
            result[row[0]] = row[1]
    return result


def get_leader_mntr(cluster, nodes):
    leader = keeper_utils.get_leader(cluster, nodes)
    data = keeper_utils.send_4lw_cmd(cluster, leader, cmd="mntr")
    return leader, parse_mntr(data)


def wait_for_follower_count(cluster, leader, expected, timeout=30):
    """Wait until the leader reports the expected number of followers via mntr."""
    start = time.time()
    while True:
        data = keeper_utils.send_4lw_cmd(cluster, leader, cmd="mntr")
        result = parse_mntr(data)
        followers = int(result.get("zk_followers", -1))
        synced = int(result.get("zk_synced_followers", -1))
        if followers == expected and synced == expected:
            return result
        if time.time() - start > timeout:
            raise AssertionError(
                f"Timed out waiting for follower count {expected}. "
                f"Got zk_followers={followers}, zk_synced_followers={synced}"
            )
        time.sleep(0.5)


def test_follower_metrics_decrease_on_stop(started_cluster):
    """When a follower is stopped, zk_followers and zk_synced_followers
    on the leader must decrease."""
    leader, mntr = get_leader_mntr(cluster, ALL_NODES)
    assert int(mntr["zk_followers"]) == 2
    assert int(mntr["zk_synced_followers"]) == 2

    # Find a follower to stop
    follower = keeper_utils.get_any_follower(cluster, ALL_NODES)
    follower.stop_clickhouse()

    # The leader should eventually report 1 follower.
    # The NuRaft response timeout is heart_beat_interval (125ms) * response_limit (20) = 2.5s,
    # so we wait a bit longer.
    result = wait_for_follower_count(cluster, leader, expected=1, timeout=30)
    assert int(result["zk_followers"]) == 1
    assert int(result["zk_synced_followers"]) == 1

    # Restart the follower and verify counts recover to 2.
    follower.start_clickhouse()
    keeper_utils.wait_until_connected(cluster, follower)
    result = wait_for_follower_count(cluster, leader, expected=2, timeout=30)
    assert int(result["zk_followers"]) == 2
    assert int(result["zk_synced_followers"]) == 2
