import csv
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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def wait_nodes():
    keeper_utils.wait_nodes(cluster, [node1, node2, node3])


def destroy_zk_client(zk):
    try:
        if zk:
            zk.stop()
            zk.close()
    except:
        pass


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def get_full_consensus_mode(node):
    data = keeper_utils.send_4lw_cmd(cluster, node, cmd="mntr")
    reader = csv.reader(data.split("\n"), delimiter="\t")
    result = {}
    for row in reader:
        if len(row) != 0:
            result[row[0]] = row[1]
    return int(result["zk_full_consensus_mode"])


def wait_full_consensus_mode(nodes, expected, resend_node, resend_cmd):
    # The mode is propagated from the leader to followers asynchronously
    # and on a best-effort basis (a busy peer may miss the propagation),
    # so poll every node and periodically re-send the command, which is
    # also the documented operator remedy for a missed propagation.
    for node in nodes:
        retry = 0
        while get_full_consensus_mode(node) != expected and retry < 30:
            if retry % 5 == 4:
                keeper_utils.send_4lw_cmd(cluster, resend_node, cmd=resend_cmd)
            time.sleep(1)
            retry += 1
        assert get_full_consensus_mode(node) == expected


BACKPRESSURE_MAX_HOLD_MS = 3000
STALE_LOG_GAP = 100


def wait_for_log(node, message, timeout=60):
    for _ in range(timeout):
        if node.contains_in_log(message):
            return
        time.sleep(1)
    raise AssertionError(f"{node.name} did not log {message!r} within {timeout} s")


def test_backpressure_not_applied_to_a_down_member(started_cluster):
    # The leader only waits for a member that can actually catch up. A member
    # that stopped responding cannot, so waiting for it would stall the whole
    # cluster for nothing.
    zk = None
    try:
        wait_nodes()

        for node in [node1, node2, node3]:
            conf = keeper_utils.send_4lw_cmd(cluster, node, cmd="conf")
            assert (
                f"slow_member_backpressure_max_hold_ms={BACKPRESSURE_MAX_HOLD_MS}"
                in conf
            )

        leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
        followers = [node for node in [node1, node2, node3] if node != leader]

        zk = get_fake_zk(leader.name)
        followers[1].stop_clickhouse()

        # Write more than `stale_log_gap` entries, so that the stopped member
        # is far enough behind to be waited for if it were eligible.
        started_at = time.monotonic()
        for i in range(STALE_LOG_GAP * 2):
            zk.create(f"/test_backpressure_down_member_{i}", b"value")
        elapsed_ms = (time.monotonic() - started_at) * 1000

        # Nowhere near the hold time per write: nothing was ever held.
        assert elapsed_ms < BACKPRESSURE_MAX_HOLD_MS, elapsed_ms
        assert not leader.contains_in_log("hold the commit index for it")

        followers[1].start_clickhouse()
        wait_nodes()
    finally:
        destroy_zk_client(zk)


def test_backpressure_holds_commit_for_lagging_member(started_cluster):
    # A member that answers heartbeats but replicates slowly is the case the
    # backpressure exists for: the leader must slow down to its speed instead
    # of leaving it behind until it needs a snapshot.
    zk = None
    try:
        wait_nodes()

        leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
        followers = [node for node in [node1, node2, node3] if node != leader]
        lagging = followers[1]

        with PartitionManager() as pm:
            # Large enough to fall behind, small enough to keep answering
            # heartbeats within `full_consensus_leader_limit` (4) heartbeats.
            pm.add_network_delay(lagging, 400)

            zk = get_fake_zk(leader.name)
            for i in range(STALE_LOG_GAP * 5):
                zk.create(f"/test_backpressure_lagging_{i}", b"value")

            wait_for_log(leader, "hold the commit index for it")

        # Once the delay is gone the member must catch up and be released,
        # rather than being left behind to recover from a snapshot.
        wait_for_log(leader, "stop holding the commit index for it")

        zk_lagging = get_fake_zk(lagging.name)
        try:
            last = STALE_LOG_GAP * 5 - 1
            assert zk_lagging.exists(f"/test_backpressure_lagging_{last}") is not None
        finally:
            destroy_zk_client(zk_lagging)
    finally:
        destroy_zk_client(zk)


def test_full_consensus_mode(started_cluster):
    zk = None
    try:
        wait_nodes()

        leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
        followers = [node for node in [node1, node2, node3] if node != leader]

        # Enable the mode through a follower to check forwarding to the leader.
        data = keeper_utils.send_4lw_cmd(cluster, followers[0], cmd="fcon")
        assert data == "Sent full consensus mode ON request to leader."
        wait_full_consensus_mode([node1, node2, node3], 1, followers[0], "fcon")

        # All members are healthy: commits require acks from all of them.
        zk = get_fake_zk(leader.name)
        zk.create("/test_full_consensus_all_healthy", b"value")
        assert zk.get("/test_full_consensus_all_healthy")[0] == b"value"

        # Stop one follower. After the exclusion window (several heartbeats)
        # the leader must exclude it and proceed with the healthy members only,
        # so writes must not be blocked for long.
        followers[1].stop_clickhouse()

        zk.create("/test_full_consensus_one_stopped", b"value")
        assert zk.get("/test_full_consensus_one_stopped")[0] == b"value"

        # The healthy follower must have the commit: in full consensus mode
        # no commit can happen without it.
        zk_follower = get_fake_zk(followers[0].name)
        assert zk_follower.get("/test_full_consensus_one_stopped")[0] == b"value"
        destroy_zk_client(zk_follower)

        # Bring the stopped follower back and disable the mode.
        followers[1].start_clickhouse()
        wait_nodes()

        data = keeper_utils.send_4lw_cmd(cluster, leader, cmd="fcof")
        assert data == "Sent full consensus mode OFF request to leader."
        wait_full_consensus_mode([node1, node2, node3], 0, leader, "fcof")

        zk.create("/test_full_consensus_disabled", b"value")
        assert zk.get("/test_full_consensus_disabled")[0] == b"value"
    finally:
        # Make sure the mode is off even if the test failed midway.
        try:
            keeper_utils.send_4lw_cmd(cluster, node1, cmd="fcof")
        except:
            pass
        destroy_zk_client(zk)
