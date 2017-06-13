import pytest
import time

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager


cluster = ClickHouseCluster(__file__)

instance_with_dist_table = cluster.add_instance('instance_with_dist_table', main_configs=['configs/remote_servers.xml'])
replica1 = cluster.add_instance('replica1', with_zookeeper=True)
replica2 = cluster.add_instance('replica2', with_zookeeper=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for replica in (replica1, replica2):
            replica.query(
                "CREATE TABLE replicated (d Date, x UInt32) ENGINE = "
                "ReplicatedMergeTree('/clickhouse/tables/replicated', '{instance}', d, d, 8192)")

        instance_with_dist_table.query(
            "CREATE TABLE distributed (d Date, x UInt32) ENGINE = "
            "Distributed('test_cluster', 'default', 'replicated')")

        yield cluster

    finally:
        cluster.shutdown()


def test(started_cluster):
    with PartitionManager() as pm:
        pm.partition_instances(replica1, replica2)

        replica2.query("INSERT INTO replicated VALUES ('2017-05-08', 1)")

        time.sleep(1) # accrue replica delay

        assert replica1.query("SELECT count() FROM replicated").strip() == ''
        assert replica2.query("SELECT count() FROM replicated").strip() == '1'

        # With in_order balancing replica1 is chosen.
        assert instance_with_dist_table.query(
            "SELECT count() FROM distributed SETTINGS load_balancing='in_order'").strip() == ''

        # When we set max_replica_delay, replica1 must be excluded.
        assert instance_with_dist_table.query('''
SELECT count() FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1
''').strip() == '1'

        pm.drop_instance_zk_connections(replica2)

        time.sleep(2.5) # allow pings to zookeeper to timeout

        # At this point all replicas are stale, but the query must still go to replica2 which is the least stale one.
        assert instance_with_dist_table.query('''
SELECT count() FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1
''').strip() == '1'

        # If we forbid stale replicas, the query must fail.
        with pytest.raises(Exception):
            print instance_with_dist_table.query('''
SELECT count() FROM distributed SETTINGS
    load_balancing='in_order',
    max_replica_delay_for_distributed_queries=1,
    fallback_to_stale_replicas_for_distributed_queries=0
''')
