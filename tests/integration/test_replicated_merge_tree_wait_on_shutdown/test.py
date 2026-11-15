#!/usr/bin/env python3

import time
from multiprocessing.dummy import Pool

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["config/merge_tree_conf.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["config/merge_tree_conf.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_shutdown_and_wait(start_cluster):
    for i, node in enumerate([node1, node2]):
        node.query(
            f"CREATE TABLE test_table (value UInt64) ENGINE=ReplicatedMergeTree('/test/table', 'r{i}') ORDER BY tuple()"
        )

    # we stop merges on node1 to make node2 fetch all 51 origin parts from node1
    # and not to fetch a smaller set of merged covering parts
    node1.query("SYSTEM STOP MERGES test_table")

    node1.query("INSERT INTO test_table VALUES (0)")
    node2.query("SYSTEM SYNC REPLICA test_table")

    assert node1.query("SELECT * FROM test_table") == "0\n"
    assert node2.query("SELECT * FROM test_table") == "0\n"

    def soft_shutdown(node):
        node.stop_clickhouse(kill=False, stop_wait_sec=60)

    p = Pool(50)

    def insert(value):
        node1.query(f"INSERT INTO test_table VALUES ({value})")

    with PartitionManager() as pm:
        pm.partition_instances(node1, node2)
        p.map(insert, range(1, 50))

        # Start shutdown async
        waiter = p.apply_async(soft_shutdown, (node1,))
        # to be sure that shutdown started
        time.sleep(5)

        # node 2 partitioned and don't see any data
        assert node2.query("SELECT * FROM test_table") == "0\n"

        # Restore network
        pm.heal_all()

    # wait for shutdown to finish
    waiter.get()

    node2.query("SYSTEM SYNC REPLICA test_table", timeout=5)

    # check second replica has all data
    assert node2.query("SELECT sum(value) FROM test_table") == "1225\n"
    # and nothing in queue
    assert node2.query("SELECT count() FROM system.replication_queue") == "0\n"

    # It can happend that the second replica is superfast
    assert node1.contains_in_log(
        "Successfully waited all the parts"
    ) or node1.contains_in_log("All parts found on replica")
