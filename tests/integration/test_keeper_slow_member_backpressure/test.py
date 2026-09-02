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


def switch_backpressure(leader, enable):
    """Switch it and wait until every node reports the new value.

    The request is forwarded to the leader and propagated from there on a
    best-effort basis, so a peer that was busy can miss it. Re-sending is what
    an operator is told to do in that case, so the test does the same.
    """
    cmd = "bpon" if enable else "bpof"
    expected = 1 if enable else 0
    for attempt in range(30):
        if attempt % 5 == 0:
            keeper_utils.send_4lw_cmd(cluster, leader, cmd=cmd)
        if all(
            get_mntr_value(node, "zk_slow_member_backpressure") == expected
            for node in NODES
        ):
            return
        time.sleep(1)
    raise AssertionError(f"not every node reports slow_member_backpressure={expected}")


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
        # Off until an operator switches it on. Without this the tests below
        # could pass against a build where it is on by default.
        assert get_mntr_value(node, "zk_slow_member_backpressure") == 0


def test_switch_reaches_every_node(started_cluster):
    # `bpon` sent to a follower has to reach the leader, which applies it and
    # propagates it. Only the leader's copy has any effect, but every node
    # reports its own, which is what an operator checks.
    keeper_utils.wait_nodes(cluster, NODES)
    leader = keeper_utils.get_leader(cluster, NODES)
    follower = next(node for node in NODES if node != leader)

    try:
        switch_backpressure(follower, True)
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
            pending = [
                zk.create_async(f"/test_backpressure_lagging_{i}", b"value")
                for i in range(500)
            ]

            wait_for_more_in_log(leader, WAITING_MESSAGE, waits_before)

            for result in pending:
                result.get(timeout=60)

        # Everything the leader accepted has to be on the lagging replica: the
        # point of waiting is that it catches up rather than being left behind.
        zk_lagging = keeper_utils.get_fake_zk(cluster, lagging.name, timeout=30.0)
        try:
            assert zk_lagging.exists("/test_backpressure_lagging_499") is not None
        finally:
            destroy_zk_client(zk_lagging)
    finally:
        destroy_zk_client(zk)
        try:
            switch_backpressure(node1, False)
        except:
            pass
