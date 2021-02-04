#!/usr/bin/env python3

import pytest
import time
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
import random
import string

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)
node3 = cluster.add_instance('node3', with_zookeeper=True, main_configs=['configs/merge_tree.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_random_string(length):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


def test_no_stall(started_cluster):
    node1.query("CREATE TABLE t (key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/test/t', '1') ORDER BY tuple() PARTITION BY key")
    node2.query("CREATE TABLE t (key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/test/t', '2') ORDER BY tuple() PARTITION BY key")
    node3.query("CREATE TABLE t (key UInt64, data String) ENGINE = ReplicatedMergeTree('/clickhouse/test/t', '3') ORDER BY tuple() PARTITION BY key")

    node1.query("SYSTEM STOP MERGES")
    node2.query("SYSTEM STOP MERGES")
    node3.query("SYSTEM STOP MERGES")

    # Pause node3 until the test setup is prepared
    node3.query("SYSTEM STOP FETCHES t")

    node1.query("INSERT INTO t SELECT 1, '{}' FROM numbers(5000)".format(get_random_string(104857)))
    node1.query("INSERT INTO t SELECT 2, '{}' FROM numbers(5000)".format(get_random_string(104857)))
    node1.query("INSERT INTO t SELECT 3, '{}' FROM numbers(5000)".format(get_random_string(104857)))
    node1.query("INSERT INTO t SELECT 4, '{}' FROM numbers(5000)".format(get_random_string(104857)))
    node1.query("INSERT INTO t SELECT 5, '{}' FROM numbers(5000)".format(get_random_string(104857)))

    # Make sure node2 has all the parts.
    node2.query("SYSTEM SYNC REPLICA t")

    # Do not allow sending from replica 2 yet, force node3 to initiate replication from node1.
    node2.query("SYSTEM STOP REPLICATED SENDS")

    print("replica 2 fully synced")

    with PartitionManager() as pm:
        # Make node1 very slow, node3 should replicate from node2 instead.
        pm.add_network_delay(node1, 2000)

        # node3 starts to replicate from node 1
        node3.query("SYSTEM START FETCHES t")

        # Wait some time to give a chance for node3 to try replicating without success from node1.
        time.sleep(10)

        # Wait for replication...
        node2.query("SYSTEM START REPLICATED SENDS")

        for _ in range(1000):
            print('Currently running fetches:')
            print(node3.query("SELECT result_part_name, source_replica_hostname, progress FROM system.replicated_fetches").strip())
            print()

            parts_fetched = node3.query("SELECT count() FROM system.parts WHERE table = 't'").strip()
            print('parts_fetched:', parts_fetched)
            print()

            # Replication done.
            if parts_fetched == "5":
                break

            time.sleep(3)

    node1.query("DROP TABLE t SYNC")
    node2.query("DROP TABLE t SYNC")
    node3.query("DROP TABLE t SYNC")
