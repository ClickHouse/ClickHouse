import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.cluster import ClickHouseKiller
from helpers.test_tools import assert_eq_with_retry
from helpers.network import PartitionManager

def fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
        '''
            CREATE DATABASE test;

            CREATE TABLE test.test_table(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/replicated', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date) SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0;
        '''.format(shard=shard, replica=node.name))

cluster = ClickHouseCluster(__file__)

node_1_1 = cluster.add_instance('node_1_1', with_zookeeper=True, main_configs=['configs/remote_servers.xml'])
node_1_2 = cluster.add_instance('node_1_2', with_zookeeper=True, main_configs=['configs/remote_servers.xml'])
node_1_3 = cluster.add_instance('node_1_3', with_zookeeper=True, main_configs=['configs/remote_servers.xml'])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        fill_nodes([node_1_1, node_1_2, node_1_3], 1)

        yield cluster

    except Exception as ex:
        print ex

    finally:
        cluster.shutdown()

def test_drop_replica(start_cluster):
    for i in range(100):
        node_1_1.query("INSERT INTO test.test_table VALUES (1, {})".format(i))

    zk = cluster.get_kazoo_client('zoo1')

    assert "can't drop local replica" in node_1_1.query_and_get_error("SYSTEM DROP REPLICA 'node_1_1' FROM test.test_table")
    assert "can't drop local replica" in node_1_2.query_and_get_error("SYSTEM DROP REPLICA 'node_1_2' FROM test.test_table")
    assert "can't drop local replica" in node_1_3.query_and_get_error("SYSTEM DROP REPLICA 'node_1_3' FROM '/clickhouse/tables/test/{shard}/replicated'".format(shard=1))
    assert "it's active" in node_1_1.query_and_get_error("SYSTEM DROP REPLICA 'node_1_2' FROM test.test_table")

    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node_1_2)

        ## make node_1_2 dead
        node_1_2.kill_clickhouse()
        time.sleep(120)
        node_1_1.query("SYSTEM DROP REPLICA 'node_1_2' FROM test.test_table")
        exists_replica_1_2 = zk.exists("/clickhouse/tables/test/{shard}/replicated/replicas/{replica}".format(shard=1, replica='node_1_2'))
        assert (exists_replica_1_2 == None)

        ## make node_1_1 dead
        node_1_1.kill_clickhouse()
        time.sleep(120)

        node_1_3.query("SYSTEM DROP REPLICA 'node_1_1' FROM '/clickhouse/tables/test/{shard}/replicated'".format(shard=1))
        exists_base_path = zk.exists("/clickhouse/tables/test/{shard}/replicated".format(shard=1))
        assert(exists_base_path == None)
