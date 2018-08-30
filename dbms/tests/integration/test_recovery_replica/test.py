import time
import pytest

from helpers.cluster import ClickHouseCluster

def fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
        '''
            CREATE DATABASE test;

            CREATE TABLE test_table(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{shard}/replicated', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date) SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0;
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
        print ex

    finally:
        cluster.shutdown()

def test_recovery(start_cluster):
    node1.query("INSERT INTO test_table VALUES (1, 1)")
    time.sleep(1)
    node2.query("DETACH TABLE test_table")

    for i in range(100):
        node1.query("INSERT INTO test_table VALUES (1, {})".format(i))

    time.sleep(2)

    node2.query("ATTACH TABLE test_table")

    time.sleep(2)

    assert node1.query("SELECT count(*) FROM test_table") == node2.query("SELECT count(*) FROM test_table")
