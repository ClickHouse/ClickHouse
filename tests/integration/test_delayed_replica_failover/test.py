import os
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)

# Cluster with 2 shards of 2 replicas each. node_1_1 is the instance with Distributed table.
# Thus we have a shard with a local replica and a shard with remote replicas.
node_1_1 = instance_with_dist_table = cluster.add_instance(
    "node_1_1", with_zookeeper=True, main_configs=["configs/remote_servers.xml"]
)
node_1_2 = cluster.add_instance("node_1_2", with_zookeeper=True)
node_2_1 = cluster.add_instance("node_2_1", with_zookeeper=True)
node_2_2 = cluster.add_instance("node_2_2", with_zookeeper=True)

# For test to be runnable multiple times
seqno = 0


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def create_tables():
    global seqno
    try:
        seqno += 1
        for shard in (1, 2):
            for replica in (1, 2):
                node = cluster.instances["node_{}_{}".format(shard, replica)]
                node.query(
                    f"CREATE TABLE replicated (d Date, x UInt32) ENGINE = "
                    f"ReplicatedMergeTree('/clickhouse/tables/{shard}/replicated_{seqno}', '{node.name}') PARTITION BY toYYYYMM(d) ORDER BY d"
                )

        node_1_1.query(
            "CREATE TABLE distributed (d Date, x UInt32) ENGINE = "
            "Distributed('test_cluster', 'default', 'replicated')"
        )

        yield

    finally:
        node_1_1.query("DROP TABLE distributed")

        node_1_1.query("DROP TABLE replicated")
        node_1_2.query("DROP TABLE replicated")
        node_2_1.query("DROP TABLE replicated")
        node_2_2.query("DROP TABLE replicated")


def test(started_cluster):
    instance_with_dist_table.query(
        "SYSTEM DISABLE FAILPOINT replicated_merge_tree_all_replicas_stale"
    )
    with PartitionManager() as pm:
        # Hinder replication between replicas of the same shard, but leave the possibility of distributed connection.
        pm.partition_instances(node_1_1, node_1_2, port=9009)
        pm.partition_instances(node_2_1, node_2_2, port=9009)

        node_1_2.query("INSERT INTO replicated VALUES ('2017-05-08', 1)")
        node_2_2.query("INSERT INTO replicated VALUES ('2017-05-08', 2)")

        time.sleep(1)  # accrue replica delay

        assert node_1_1.query("SELECT sum(x) FROM replicated").strip() == "0"
        assert node_1_2.query("SELECT sum(x) FROM replicated").strip() == "1"
        assert node_2_1.query("SELECT sum(x) FROM replicated").strip() == "0"
        assert node_2_2.query("SELECT sum(x) FROM replicated").strip() == "2"

        # With in_order balancing first replicas are chosen.
        assert (
            instance_with_dist_table.query(
                "SELECT count() FROM distributed SETTINGS load_balancing='in_order'"
            ).strip()
            == "0"
        )

        # When we set max_replica_delay, first replicas must be excluded.
        assert (
            instance_with_dist_table.query(
                """
SELECT sum(x) FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1
"""
            ).strip()
            == "3"
        )

        assert (
            instance_with_dist_table.query(
                """
SELECT sum(x) FROM distributed WITH TOTALS SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1
"""
            ).strip()
            == "3\n\n3"
        )

        pm.drop_instance_zk_connections(node_1_2)
        pm.drop_instance_zk_connections(node_2_2)

        # allow pings to zookeeper to timeout (must be greater than ZK session timeout).
        for _ in range(30):
            try:
                node_2_2.query(
                    "SELECT * FROM system.zookeeper where path = '/' SETTINGS insert_keeper_max_retries = 0"
                )
                time.sleep(0.5)
            except:
                break
        else:
            raise Exception("Connection with zookeeper was not lost")

        instance_with_dist_table.query(
            "SYSTEM ENABLE FAILPOINT replicated_merge_tree_all_replicas_stale"
        )
        # At this point all replicas are stale, but the query must still go to second replicas which are the least stale ones.
        assert (
            instance_with_dist_table.query(
                """
SELECT sum(x) FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1
"""
            ).strip()
            == "3"
        )

        # Prefer fallback_to_stale_replicas over skip_unavailable_shards
        assert (
            instance_with_dist_table.query(
                """
SELECT sum(x) FROM distributed SETTINGS
    load_balancing='in_order',
    skip_unavailable_shards=1,
    max_replica_delay_for_distributed_queries=1
"""
            ).strip()
            == "3"
        )

        # If we forbid stale replicas, the query must fail. But sometimes we must have bigger timeouts.
        for _ in range(20):
            try:
                instance_with_dist_table.query(
                    """
SELECT count() FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1,
    fallback_to_stale_replicas_for_distributed_queries=0
"""
                )
                time.sleep(0.5)
            except:
                break
        else:
            raise Exception("Didn't raise when stale replicas are not allowed")

        # Now partition off the remote replica of the local shard and test that failover still works.
        pm.partition_instances(node_1_1, node_1_2, port=9000)

        assert (
            instance_with_dist_table.query(
                """
SELECT sum(x) FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1
"""
            ).strip()
            == "2"
        )
