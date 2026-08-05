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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def wait_nodes():
    keeper_utils.wait_nodes(cluster, [node1, node2, node3])


def get_mntr_metrics(node):
    data = keeper_utils.send_4lw_cmd(cluster, node, cmd="mntr")
    reader = csv.reader(data.split("\n"), delimiter="\t")
    result = {}
    for row in reader:
        if len(row) != 0:
            result[row[0]] = row[1]
    return result


def test_leader_failover_metrics(started_cluster):
    """The election / leader-unavailability metrics must actually advance on
    the node that wins a re-election after a real no-leader window, not just
    exist in `mntr` output."""
    old_leader = None
    try:
        wait_nodes()

        old_leader = keeper_utils.get_leader(cluster, [node1, node2, node3])
        survivors = [n for n in [node1, node2, node3] if n.name != old_leader.name]

        # Zero the cumulative counters on the future election winner so the
        # assertions below measure only this failover.
        for node in survivors:
            keeper_utils.send_4lw_cmd(cluster, node, cmd="srst")

        # Kill the leader to force a real no-leader window and a re-election.
        old_leader.stop_clickhouse(kill=True)

        new_leader = None
        for _ in range(60):
            try:
                new_leader = keeper_utils.get_leader(cluster, survivors)
                break
            except Exception:
                time.sleep(1)
        assert (
            new_leader is not None
        ), "No new leader was elected after the old leader was killed"

        # The election window is closed in the `BecomeLeader` callback and the
        # leader-unavailability window at the next poll tick, so retry briefly.
        result = {}
        for _ in range(60):
            result = get_mntr_metrics(new_leader)
            if (
                int(result.get("zk_cnt_election_time", 0)) >= 1
                and int(result.get("zk_cnt_leader_unavailable_time", 0)) >= 1
            ):
                break
            time.sleep(1)

        assert int(result["zk_cnt_election_time"]) >= 1
        assert int(result["zk_sum_election_time"]) > 0
        assert int(result["zk_cnt_leader_unavailable_time"]) >= 1
        assert int(result["zk_sum_leader_unavailable_time"]) > 0
        assert int(result["zk_leader_uptime"]) >= 0
    finally:
        if old_leader is not None:
            old_leader.start_clickhouse()
        wait_nodes()
