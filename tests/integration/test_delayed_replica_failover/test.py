import pytest
import time
import os, sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import helpers

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager


cluster = ClickHouseCluster(__file__)

# Cluster with 2 shards of 2 replicas each. node_1_1 is the instance with Distributed table.
# Thus we have a shard with a local replica and a shard with remote replicas.
node_1_1 = instance_with_dist_table = cluster.add_instance(
    'node_1_1', with_zookeeper=True, main_configs=['configs/remote_servers.xml'])
node_1_2 = cluster.add_instance('node_1_2', with_zookeeper=True)
node_2_1 = cluster.add_instance('node_2_1', with_zookeeper=True)
node_2_2 = cluster.add_instance('node_2_2', with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for shard in (1, 2):
            for replica in (1, 2):
                node = cluster.instances['node_{}_{}'.format(shard, replica)]
                node.query('''
CREATE TABLE replicated (d Date, x UInt32) ENGINE =
    ReplicatedMergeTree('/clickhouse/tables/{shard}/replicated', '{instance}', d, d, 8192)'''
                    .format(shard=shard, instance=node.name))

        node_1_1.query(
            "CREATE TABLE distributed (d Date, x UInt32) ENGINE = "
            "Distributed('test_cluster', 'default', 'replicated')")

        yield cluster

    finally:
        cluster.shutdown()


def test(started_cluster):
    with PartitionManager() as pm:
        # Hinder replication between replicas of the same shard, but leave the possibility of distributed connection.
        pm.partition_instances(node_1_1, node_1_2, port=9009)
        pm.partition_instances(node_2_1, node_2_2, port=9009)

        node_1_2.query("INSERT INTO replicated VALUES ('2017-05-08', 1)")
        node_2_2.query("INSERT INTO replicated VALUES ('2017-05-08', 2)")

        time.sleep(1) # accrue replica delay

        assert node_1_1.query("SELECT sum(x) FROM replicated").strip() == '0'
        assert node_1_2.query("SELECT sum(x) FROM replicated").strip() == '1'
        assert node_2_1.query("SELECT sum(x) FROM replicated").strip() == '0'
        assert node_2_2.query("SELECT sum(x) FROM replicated").strip() == '2'

        # With in_order balancing first replicas are chosen.
        assert instance_with_dist_table.query(
            "SELECT count() FROM distributed SETTINGS load_balancing='in_order'").strip() == '0'

        # When we set max_replica_delay, first replicas must be excluded.
        assert instance_with_dist_table.query('''
SELECT sum(x) FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1
''').strip() == '3'

        pm.drop_instance_zk_connections(node_1_2)
        pm.drop_instance_zk_connections(node_2_2)

        time.sleep(4) # allow pings to zookeeper to timeout (must be greater than ZK session timeout).

        # At this point all replicas are stale, but the query must still go to second replicas which are the least stale ones.
        assert instance_with_dist_table.query('''
SELECT sum(x) FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1
''').strip() == '3'

        # If we forbid stale replicas, the query must fail.
        with pytest.raises(Exception):
            print instance_with_dist_table.query('''
SELECT count() FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1,
    fallback_to_stale_replicas_for_distributed_queries=0
''')

        # Now partition off the remote replica of the local shard and test that failover still works.
        pm.partition_instances(node_1_1, node_1_2, port=9000)

        assert instance_with_dist_table.query('''
SELECT sum(x) FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1
''').strip() == '2'
