import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

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


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        fill_nodes([node_1_1, node_1_2], 1)

        yield cluster

    except Exception as ex:
        print ex

    finally:
        cluster.shutdown()

def test_drop_replica(start_cluster):
    for i in range(100):
        node_1_1.query("INSERT INTO test.test_table VALUES (1, {})".format(i))

    node_1_2.kill_clickhouse()

    zk = cluster.get_kazoo_client('zoo1')
    node_1_1.query("ALTER TABLE test.test_table drop replica 'node_1_2'")
    e2 = zk.exists("/clickhouse/tables/test/{shard}/replicated/replicas/{replica}".format(shard=1, replica='node_1_2'))

    assert(e2 == False)

    node_1_1.query("ALTER TABLE test.test_table drop replica 'node_1_1'")
    exists_base_path = zk.exists("/clickhouse/tables/test/{shard}/replicated".format(shard=1))

    assert(exists_base_path == False)
