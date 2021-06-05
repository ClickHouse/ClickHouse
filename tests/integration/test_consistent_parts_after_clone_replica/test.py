import pytest

from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager
from helpers.test_tools import assert_eq_with_retry
import time


def fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
            '''
            CREATE DATABASE test;
            CREATE TABLE test_table(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{shard}/replicated', '{replica}')
            ORDER BY id PARTITION BY toYYYYMM(date) 
            SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0;
            '''.format(shard=shard, replica=node.name))


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        fill_nodes([node1, node2], 1)
        yield cluster
    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()


def test_inconsistent_parts_if_drop_while_replica_not_active(start_cluster):
    with PartitionManager() as pm:
        # insert into all replicas
        for i in range(10):
            node1.query("INSERT INTO test_table VALUES ('2019-08-16', {})".format(i))
        assert_eq_with_retry(node2, "SELECT count(*) FROM test_table", node1.query("SELECT count(*) FROM test_table"))

        # partition the first replica from the second one and (later) from zk
        pm.partition_instances(node1, node2)

        # insert some parts on the second replica only, we will drop these parts
        for i in range(10):
            node2.query("INSERT INTO test_table VALUES ('2019-08-16', {})".format(10 + i))

        pm.drop_instance_zk_connections(node1)

        # drop all parts on the second replica
        node2.query_with_retry("ALTER TABLE test_table DROP PARTITION 201908")
        assert_eq_with_retry(node2, "SELECT count(*) FROM test_table", "0")

        # insert into the second replica
        # DROP_RANGE will be removed from the replication log and the first replica will be lost
        for i in range(20):
            node2.query("INSERT INTO test_table VALUES ('2019-08-16', {})".format(20 + i))
        
        assert_eq_with_retry(node2, "SELECT value FROM system.zookeeper WHERE path='/clickhouse/tables/test1/replicated/replicas/node1' AND name='is_lost'", "1")

        for i in range(30):
            if node2.contains_in_log("Will mark replica node1 as lost"):
                break
            time.sleep(0.5)

        # the first replica will be cloned from the second
        pm.heal_all()
        assert_eq_with_retry(node1, "SELECT count(*) FROM test_table", node2.query("SELECT count(*) FROM test_table"))

        # ensure replica was cloned
        assert node1.contains_in_log("Will mimic node2")
        # queue must be empty (except some merges that are possibly executing right now)
        assert node1.query("SELECT count() FROM system.replication_queue WHERE type != 'MERGE_PARTS'") == "0\n"
        assert node2.query("SELECT count() FROM system.replication_queue WHERE type != 'MERGE_PARTS'") == "0\n"
