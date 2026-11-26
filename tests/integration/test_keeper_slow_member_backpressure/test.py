import csv
import re
import time

import pytest

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

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

NODES = [node1, node2, node3]

# The message the leader logs while it is holding the commit index back. It is
# rate limited to one every five seconds, and the first one is always written.
WAITING_MESSAGE = "slow member backpressure: waiting for peer"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def destroy_zk_client(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def get_mntr_value(node, key):
    data = keeper_utils.send_4lw_cmd(cluster, node, cmd="mntr")
    reader = csv.reader(data.split("\n"), delimiter="\t")
    for row in reader:
        if len(row) != 0 and row[0] == key:
            return int(row[1])
    raise AssertionError(f"{key} not reported by {node.name}: {data}")


def switch_backpressure(send_to, enable):
    """Switch it and wait until the leader reports the new value.

    Only the leader holds the setting; a follower asked for it forwards the
    request and does not switch anything locally. The request can be lost if
    the leader connection is busy, so re-send while waiting.
    """
    cmd = "bpon" if enable else "bpof"
    expected = 1 if enable else 0
    for attempt in range(30):
        if attempt % 5 == 0:
            keeper_utils.send_4lw_cmd(cluster, send_to, cmd=cmd)
        leader = keeper_utils.get_leader(cluster, NODES)
        if get_mntr_value(leader, "zk_slow_member_backpressure") == expected:
            return
        time.sleep(1)
    raise AssertionError(f"leader does not report slow_member_backpressure={expected}")


def wait_for_more_in_log(node, message, was, timeout=60):
    # Counted rather than matched, because the cluster is shared by every test
    # in this module and an earlier test may have logged the same line.
    for _ in range(timeout):
        if node.count_in_log(message) > was:
            return
        time.sleep(1)
    raise AssertionError(
        f"{node.name} did not log {message!r} more than {was} times within {timeout} s"
    )


def test_setting_is_reported(started_cluster):
    keeper_utils.wait_nodes(cluster, NODES)

    for node in NODES:
        conf = keeper_utils.send_4lw_cmd(cluster, node, cmd="conf")
        assert "slow_member_backpressure_max_uncommitted_log_entries=200" in conf
        assert "slow_member_backpressure_no_progress_timeout_ms=5000" in conf
        # Off until an operator switches it on. Without this the tests below
        # could pass against a build where it is on by default.
        assert get_mntr_value(node, "zk_slow_member_backpressure") == 0


def test_switch_reaches_the_leader(started_cluster):
    # `bpon` sent to a follower has to reach the leader, which is the only node
    # that holds the setting. The followers must not start reporting it: only
    # the leader's copy has any effect, so a follower reporting it on would
    # tell an operator the cluster is throttled when it may not be.
    keeper_utils.wait_nodes(cluster, NODES)
    leader = keeper_utils.get_leader(cluster, NODES)
    follower = next(node for node in NODES if node != leader)

    try:
        switch_backpressure(follower, True)
        for node in NODES:
            if node != leader:
                assert get_mntr_value(node, "zk_slow_member_backpressure") == 0
    finally:
        switch_backpressure(leader, False)


def test_reply_says_only_what_the_node_can_know(started_cluster):
    # Only the leader holds the setting, so only the leader can answer that it
    # is on. A follower has done no more than send the request: the leader
    # refuses one that arrives after it has stopped leading, so a reply from a
    # follower promising that writes are throttled would be a claim the node is
    # in no position to make.
    keeper_utils.wait_nodes(cluster, NODES)
    leader = keeper_utils.get_leader(cluster, NODES)
    follower = next(node for node in NODES if node != leader)

    try:
        reply = keeper_utils.send_4lw_cmd(cluster, follower, cmd="bpon")
        assert "Sent slow member backpressure ON request to leader" in reply
        assert "Writes now commit" not in reply

        reply = keeper_utils.send_4lw_cmd(cluster, leader, cmd="bpon")
        assert "Slow member backpressure is ON" in reply
        assert "Writes now commit at the speed of the slowest" in reply
    finally:
        switch_backpressure(leader, False)


def test_unreachable_replica_does_not_block_writes(started_cluster):
    # The leader waits only for a replica it can reach. A stopped replica can
    # never catch up, so waiting for it would freeze the commit index and take
    # the cluster down for writes - with the backpressure on and no time limit,
    # nothing else would ever release it.
    zk = None
    stopped = None
    try:
        keeper_utils.wait_nodes(cluster, NODES)
        leader = keeper_utils.get_leader(cluster, NODES)

        switch_backpressure(leader, True)

        stopped = next(node for node in NODES if node != leader)
        stopped.stop_clickhouse()

        zk = keeper_utils.get_fake_zk(cluster, leader.name, timeout=30.0)
        for i in range(200):
            zk.create(f"/test_backpressure_unreachable_{i}", b"value")
        assert zk.exists("/test_backpressure_unreachable_199") is not None
    finally:
        destroy_zk_client(zk)
        if stopped is not None:
            stopped.start_clickhouse()
            keeper_utils.wait_nodes(cluster, NODES)
        try:
            switch_backpressure(node1, False)
        except:
            pass


def last_clamp_from_log(node):
    """The held index and what the quorum would have committed, from the last
    waiting message the leader wrote."""
    lines = [
        line
        for line in node.grep_in_log(WAITING_MESSAGE).split("\n")
        if WAITING_MESSAGE in line
    ]
    assert lines, f"{node.name} logged no {WAITING_MESSAGE!r}"
    match = re.search(
        r"waiting for peer \d+ at log index (\d+),.*"
        r"quorum would commit up to (\d+)",
        lines[-1],
    )
    assert match, f"unexpected message format: {lines[-1]}"
    return int(match.group(1)), int(match.group(2))


def test_leader_waits_for_a_lagging_replica(started_cluster):
    # A replica that answers heartbeats but replicates slowly is the case the
    # backpressure exists for. The leader must run at its speed rather than
    # leaving it behind until it needs a snapshot.
    zk = None
    try:
        keeper_utils.wait_nodes(cluster, NODES)
        leader = keeper_utils.get_leader(cluster, NODES)
        lagging = next(node for node in NODES if node != leader)

        switch_backpressure(leader, True)
        waits_before = leader.count_in_log(WAITING_MESSAGE)

        with PartitionManager() as pm:
            # Large enough to fall behind, small enough to keep answering
            # within the four heartbeat intervals that count as reachable.
            pm.add_network_delay(lagging, 300)

            zk = keeper_utils.get_fake_zk(cluster, leader.name, timeout=30.0)
            # Asynchronous, so that the requests pipeline. A synchronous write
            # waits for its own commit, which never lets the leader get ahead
            # of anyone, so no replica could ever be seen lagging.
            #
            # Whether these succeed is deliberately not asserted. While the
            # backpressure is on, writes go at the speed of the delayed
            # replica and the leader refuses everything past
            # `slow_member_backpressure_max_uncommitted_log_entries`, so
            # clients are expected to see timeouts. That is the feature
            # working, not a failure.
            for i in range(500):
                zk.create_async(f"/test_backpressure_lagging_{i}", b"value")

            wait_for_more_in_log(leader, WAITING_MESSAGE, waits_before)

            # The leader reports where it held the commit index and where the
            # quorum would have taken it. The gap between them is the
            # backpressure doing its job: without it the quorum of the leader
            # and the third replica would have committed further.
            held_at, quorum_would_commit = last_clamp_from_log(leader)
            assert quorum_would_commit > held_at, (held_at, quorum_would_commit)

        destroy_zk_client(zk)
        zk = None
        switch_backpressure(node1, False)

        # Once the delay is gone and the backpressure is off, the cluster has
        # to be fully functional again and the replica that was waited for has
        # to have caught up rather than been left behind.
        keeper_utils.wait_nodes(cluster, NODES)
        zk = keeper_utils.get_fake_zk(cluster, leader.name, timeout=30.0)
        zk.create("/test_backpressure_recovered", b"value")

        zk_lagging = keeper_utils.get_fake_zk(cluster, lagging.name, timeout=30.0)
        try:
            zk_lagging.sync("/test_backpressure_recovered")
            assert zk_lagging.exists("/test_backpressure_recovered") is not None
        finally:
            destroy_zk_client(zk_lagging)
    finally:
        destroy_zk_client(zk)
        try:
            switch_backpressure(node1, False)
        except:
            pass
