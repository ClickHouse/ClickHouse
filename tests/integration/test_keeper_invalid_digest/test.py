#!/usr/bin/env python3

import time
import pytest
from multiprocessing.dummy import Pool

import helpers.keeper_utils as keeper_utils
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_nodes():
    keeper_utils.wait_nodes(cluster, [node1, node2])


def get_fake_zk(nodename, timeout=30.0):
    return keeper_utils.get_fake_zk(cluster, nodename, timeout=timeout)


def start_clickhouse(node):
    node.start_clickhouse()


def setup_nodes():
    node1.stop_clickhouse()
    node2.stop_clickhouse()

    p = Pool(2)
    waiters = []
    for node in [node1, node2]:
        node.exec_in_container(["rm", "-rf", "/var/lib/clickhouse/coordination/log"])
        node.exec_in_container(
            ["rm", "-rf", "/var/lib/clickhouse/coordination/snapshots"]
        )

        waiters.append(p.apply_async(start_clickhouse, (node,)))

    for waiter in waiters:
        waiter.wait()

    wait_nodes()


def test_keeper_invalid_digest(started_cluster):
    if (
        node1.is_built_with_thread_sanitizer()
        or node1.is_built_with_address_sanitizer()
        or node1.is_built_with_memory_sanitizer()
    ):
        pytest.skip("doesn't fit in timeouts for stacktrace generation")

    try:
        # Wait for the cluster to be ready
        setup_nodes()

        # Enable the failpoint on node1 to set invalid digest
        node1.query("SYSTEM ENABLE FAILPOINT keeper_leader_sets_invalid_digest")

        # Create a zookeeper connection to node1
        node1_zk = get_fake_zk("node1")

        # Perform an operation that will trigger the failpoint
        # The second node should detect the invalid digest and abort
        node1_zk.create_async("/test_invalid_digest", b"testdata")

        node2.wait_for_log_line("Digest for nodes is not matching after preprocessing request of type 'Create' at log index")
        node2.stop_clickhouse(kill=True)

        def get_last_committed_log_idx():
            return int(
                next(
                    filter(
                        lambda line: line.startswith("last_committed_log_idx"),
                        keeper_utils.send_4lw_cmd(cluster, node1, "lgif").split("\n"),
                    )
                ).split("\t")[1]
            )

        last_committed_log_idx = get_last_committed_log_idx()
        node2.start_clickhouse(expected_to_fail=True)
        node2.wait_for_log_line("Digest for nodes is not matching after preprocessing request of type 'Create' at log index")
        assert get_last_committed_log_idx() == last_committed_log_idx
    finally:
        # Clean up connections
        try:
            if "node1_zk" in locals():
                node1_zk.stop()
                node1_zk.close()
        except:
            pass
