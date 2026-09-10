#!/usr/bin/env python3
"""
A Keeper member that is removed from the cluster and later added back, without its
process being restarted in between, must rejoin and keep serving. It must not exit while
doing so, and it must not lose what it had already committed.

The process staying up across the removal is the whole point: a restarted member comes
back with a state machine it reloads from disk, which is a different situation from one
that has been holding the same live state machine the entire time.
"""

import typing as tp

import pytest

import helpers.keeper_utils as ku
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1", main_configs=["configs/keeper1.xml"], with_zookeeper=True, stay_alive=True
)
node2 = cluster.add_instance(
    "node2", main_configs=["configs/keeper2.xml"], with_zookeeper=True, stay_alive=True
)
node3 = cluster.add_instance(
    "node3", main_configs=["configs/keeper3.xml"], with_zookeeper=True, stay_alive=True
)

zk1: tp.Optional[ku.KeeperClient] = None
zk3: tp.Optional[ku.KeeperClient] = None


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        for conn in [zk1, zk3]:
            if conn:
                conn.stop()
        cluster.shutdown()


def create_client(node: ClickHouseInstance):
    return ku.KeeperClient(
        cluster.server_bin_path, cluster.get_instance_ip(node.name), 9181
    )


def committed_index(node) -> int:
    """`last_committed_log_idx` as this node itself reports it."""
    for line in ku.send_4lw_cmd(cluster, node, "lgif").split("\n"):
        key, _, value = line.partition("\t")
        if key == "last_committed_log_idx":
            return int(value)
    raise AssertionError("lgif did not report last_committed_log_idx")


def test_rejoin_removed_member_without_restart(started_cluster):
    global zk1, zk3

    # Wait before connecting: a client built against a keeper that has not finished coming
    # up blocks in its constructor rather than failing.
    for node in [node1, node2, node3]:
        ku.wait_until_connected(cluster, node)

    zk1 = create_client(node1)
    zk3 = create_client(node3)

    # Give every member something committed of its own, so the removal below leaves
    # node3 holding state rather than starting from nothing.
    for i in range(20):
        zk1.create(f"/before_removal_{i}", "somedata")
    zk3.sync("/before_removal_0")
    assert zk3.exists("/before_removal_19")

    config = zk1.reconfig(joining=None, leaving="3", new_members=None)
    assert "node3" not in config

    # Wait for node3 to see the removal itself, rather than trusting that `reconfig`
    # returning says anything about its local state, and take its index only once it has
    # stopped moving. Without this the comparison further down is a race.
    node3.wait_for_log_line("has been removed from the cluster", timeout=60)
    removed_at = committed_index(node3)
    for _ in range(30):
        settled = committed_index(node3)
        if settled == removed_at:
            break
        removed_at = settled
    assert removed_at > 0

    # Move the remaining two well past where node3 stopped.
    for i in range(50):
        zk1.create(f"/after_removal_{i}", "somedata")
    assert committed_index(node1) > removed_at

    # Add it back. Its process has been up throughout.
    config = zk1.reconfig(joining="server.3=node3:9234", leaving=None, new_members=None)
    assert "node3" in config

    ku.wait_until_connected(cluster, node3)
    zk3.stop()
    zk3 = create_client(node3)
    # Serving requests is not the same as having converged on the membership: wait for
    # node3's own view of the configuration to match the one the reconfiguration returned.
    ku.wait_configs_equal(config, zk3)
    zk3.sync("/after_removal_49")

    # It caught up rather than restarting from the beginning or falling over.
    assert committed_index(node3) > removed_at
    assert zk3.exists("/after_removal_49")
    assert zk3.exists("/before_removal_0")

    # And it is a full member again, not merely readable: a write through it is accepted
    # and the rest of the group sees it.
    zk3.create("/after_rejoin", "somedata")
    zk1.sync("/after_rejoin")
    assert zk1.exists("/after_rejoin")

    assert node3.contains_in_log("Trying to commit a ZXID") is False
    assert node3.contains_in_log("Trying to rollback invalid ZXID") is False
