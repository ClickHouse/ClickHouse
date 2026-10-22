import os
import sys
import time
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry

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
        # Insert initial records and wait until they are replicated across all nodes
        node_1_2.query("INSERT INTO replicated VALUES ('2017-05-08', 1)")
        node_2_2.query("INSERT INTO replicated VALUES ('2017-05-08', 2)")

        for _ in range(100):
            count = node_1_1.query(
                "SELECT count() FROM clusterAllReplicas(test_cluster, default.replicated)"
            ).strip()
            if count == "4":
                break
        else:
            raise Exception("Failed to replicate initial records")

        # Hinder replication between replicas of the same shard, but leave the possibility of distributed connection.
        pm.partition_instances(node_1_1, node_1_2, port=9009)
        pm.partition_instances(node_2_1, node_2_2, port=9009)

        # Insert additional records which won't be replicated
        node_1_2.query("INSERT INTO replicated VALUES ('2017-05-08', 3)")
        node_2_2.query("INSERT INTO replicated VALUES ('2017-05-08', 4)")

        time.sleep(1)  # accrue replica delay

        assert node_1_1.query("SELECT sum(x) FROM replicated").strip() == "1"
        assert node_1_2.query("SELECT sum(x) FROM replicated").strip() == "4"
        assert node_2_1.query("SELECT sum(x) FROM replicated").strip() == "2"
        assert node_2_2.query("SELECT sum(x) FROM replicated").strip() == "6"

        # With in_order balancing first replicas are chosen.
        assert (
            instance_with_dist_table.query(
                "SELECT sum(x) FROM distributed SETTINGS load_balancing='in_order'"
            ).strip()
            == "3"
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
            == "10"
        )

        assert (
            instance_with_dist_table.query(
                """
SELECT sum(x) FROM distributed WITH TOTALS SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1
"""
            ).strip()
            == "10\n\n10"
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
            == "10"
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
            == "10"
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
            == "7"
        )


def test_global_in_pushed_down_to_stale_local_replica(started_cluster):
    # A GLOBAL IN pushed down into the query of a shard with a stale local replica must use the initiator's set.
    node_1_1.query("SYSTEM DISABLE FAILPOINT replicated_merge_tree_all_replicas_stale")
    try:
        node_1_2.query(
            "INSERT INTO replicated VALUES ('2017-05-08', 1), ('2017-05-08', 2), ('2017-05-08', 3)"
        )
        node_1_1.query("SYSTEM SYNC REPLICA replicated")
        for node, values in ((node_1_1, "(1), (4)"), (node_1_2, "(1), (2)")):
            node.query("CREATE TABLE ids (y UInt32) ENGINE = Memory")
            node.query(f"INSERT INTO ids VALUES {values}")

        node_1_1.query("SYSTEM STOP FETCHES replicated")
        node_1_2.query("INSERT INTO replicated VALUES ('2017-05-08', 4)")
        assert_eq_with_retry(
            node_1_1,
            "SELECT absolute_delay >= 2 FROM system.replicas WHERE database = 'default' AND table = 'replicated'",
            "1",
        )

        query = """
SELECT x FROM (SELECT * FROM distributed) WHERE x {} (SELECT y FROM ids) ORDER BY x SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1,
    prefer_localhost_replica=1,
    serialize_query_plan=0,
    allow_push_predicate_ast_for_distributed_subqueries=1
"""
        for op, function, expected in (
            ("GLOBAL IN", "globalIn", ["1", "4"]),
            ("GLOBAL NOT IN", "globalNotIn", ["2", "3"]),
        ):
            query_id = uuid.uuid4().hex
            assert (
                node_1_1.query(query.format(op), query_id=query_id).split() == expected
            )
            # Rows above + a pushed-down filter on the fresh replica mean it used the initiator's set.
            node_1_2.query("SYSTEM FLUSH LOGS query_log")
            shard_query = node_1_2.query(
                "SELECT query FROM system.query_log WHERE type = 'QueryFinish' AND is_initial_query = 0 "
                f"AND initial_query_id = '{query_id}' FORMAT TSVRaw"
            )
            assert f"{function}(" in shard_query, shard_query
    finally:
        node_1_1.query("SYSTEM START FETCHES replicated")
        for node in (node_1_1, node_1_2):
            node.query("DROP TABLE IF EXISTS ids SYNC")
