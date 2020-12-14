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

            CREATE TABLE test.test_table(num UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/test_table', '{replica}')
            ORDER BY num PARTITION BY intDiv(num, 1000);
        '''.format(shard=shard, replica=node.name))

cluster = ClickHouseCluster(__file__)
configs =["config/remote_servers.xml"]

node_1_1 = cluster.add_instance('node_1_1', with_zookeeper=True, main_configs=configs)
node_1_2 = cluster.add_instance('node_1_2', with_zookeeper=True, main_configs=configs)
node_1_3 = cluster.add_instance('node_1_3', with_zookeeper=True, main_configs=configs)

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

def test_restore_replica(start_cluster):
    zk = cluster.get_kazoo_client('zoo1')

    node_1_1.query("INSERT INTO test.test_table SELECT * FROM numbers(1000, 2000)")

    # 1. delete metadata
    zk.delete("/clickhouse/tables/test/01/test_table", recursive=True)
    assert zk.exists("/clickhouse/tables/test/01/test_table") is None
    time.sleep(10)

    # 2. Assert there is an exception
    node_1_1.query("SYSTEM RESTART REPLICA test.test_table")
    node_1_1.query_and_get_error("INSERT INTO test.test_table SELECT * FROM numbers(1000,2000)")

    # 3. restore replica
    node_1_1.query("SYSTEM RESTORE REPLICA test.test_table")

    # 4. check if the data is present
