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


def get_mntr_flag(node, key):
    data = keeper_utils.send_4lw_cmd(cluster, node, cmd="mntr")
    reader = csv.reader(data.split("\n"), delimiter="\t")
    result = {}
    for row in reader:
        if len(row) != 0:
            result[row[0]] = row[1]
    return int(result[key])


def enable_backpressure(nodes, resend_node, enable):
    # The backpressure is off until an operator turns it on, and the request is
    # forwarded to the leader and propagated from there on a best-effort basis,
    # so poll every node and re-send periodically.
    cmd = "bpon" if enable else "bpof"
    expected = 1 if enable else 0
    keeper_utils.send_4lw_cmd(cluster, resend_node, cmd=cmd)
    for node in nodes:
        retry = 0
        while get_mntr_flag(node, "zk_slow_member_backpressure") != expected and retry < 30:
            if retry % 5 == 4:
                keeper_utils.send_4lw_cmd(cluster, resend_node, cmd=cmd)
            time.sleep(1)
            retry += 1
        assert get_mntr_flag(node, "zk_slow_member_backpressure") == expected


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
                f"slow_member_backpressure_max_duration_ms={BACKPRESSURE_MAX_HOLD_MS}"
                in conf
            )

        leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
        followers = [node for node in [node1, node2, node3] if node != leader]

        # Without this the test would pass with the feature simply switched off.
        enable_backpressure([node1, node2, node3], leader, True)

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
        try:
            keeper_utils.send_4lw_cmd(cluster, node1, cmd="bpof")
        except:
            pass
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

        enable_backpressure([node1, node2, node3], leader, True)

        holds_before = leader.count_in_log("hold the commit index for it")
        releases_before = leader.count_in_log("stop holding the commit index for it")

        with PartitionManager() as pm:
            # Large enough to fall behind, small enough to keep answering
            # heartbeats within `full_consensus_leader_limit` (4) heartbeats.
            pm.add_network_delay(lagging, 300)

            zk = get_fake_zk(leader.name)
            # Asynchronous, so that the requests pipeline: a synchronous write
            # waits for its own commit, which never lets the leader get ahead
            # of anyone and so can never produce a lagging member.
            pending = [
                zk.create_async(f"/test_backpressure_lagging_{i}", b"value")
                for i in range(STALE_LOG_GAP * 20)
            ]

            wait_for_more_in_log(
                leader, "hold the commit index for it", holds_before
            )

            for result in pending:
                result.get(timeout=60)

        # Once the delay is gone the member must catch up and be released,
        # rather than being left behind to recover from a snapshot.
        wait_for_more_in_log(
            leader, "stop holding the commit index for it", releases_before
        )

        zk_lagging = get_fake_zk(lagging.name)
        try:
            last = STALE_LOG_GAP * 20 - 1
            assert zk_lagging.exists(f"/test_backpressure_lagging_{last}") is not None
        finally:
            destroy_zk_client(zk_lagging)
    finally:
        try:
            keeper_utils.send_4lw_cmd(cluster, node1, cmd="bpof")
        except:
            pass
        destroy_zk_client(zk)
