import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.cluster import ClickHouseKiller
from helpers.test_tools import assert_eq_with_retry
from helpers.network import PartitionManager

def fill_nodes(nodes):
    for node in nodes:
        node.query(
        '''
            CREATE DATABASE test;

            CREATE TABLE test.test_table(n UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/1/test_table', '{replica}')
            ORDER BY n PARTITION BY intDiv(n, 1000);
        '''.format(replica=node.name))

cluster = ClickHouseCluster(__file__)
configs =["configs/remote_servers.xml"]

node_1_1 = cluster.add_instance('node_1_1', with_zookeeper=True, main_configs=configs)
node_1_2 = cluster.add_instance('node_1_2', with_zookeeper=True, main_configs=configs)
node_1_3 = cluster.add_instance('node_1_3', with_zookeeper=True, main_configs=configs)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        fill_nodes([node_1_1, node_1_2, node_1_3])
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()

def test_restore_replica(start_cluster):
    def check_data():
        res: str = node_1_1.query("SELECT sum(n), count(n) FROM test.test_table")
        assert res == node_1_2.query("SELECT sum(n), count(n) FROM test.test_table")
        assert res == node_1_3.query("SELECT sum(n), count(n) FROM test.test_table")

    zk = cluster.get_kazoo_client('zoo1')

    node_1_1.query("INSERT INTO test.test_table SELECT * FROM numbers(1000, 2000)")

    # 0. Assert all the replicas have the data
    check_data()

    print(node_1_1.query(
        "SELECT partition, sum(rows) AS rows, count() FROM system.parts WHERE"
        " table = 'test_table' AND active GROUP BY partition"))

    print(node_1_1.query("SELECT * FROM system.zookeeper WHERE path = '/clickhouse/tables/test/1/'"))

    # 1. Delete metadata (emulating a Zookeeper error)
    zk.delete("/clickhouse/tables/test/1/test_table", recursive=True)
    assert zk.exists("/clickhouse/tables/test/1/test_table") is None

    node_1_1.query("SYSTEM RESTART REPLICA test.test_table")

    # 2. Assert there is an exception as the metadata is missing
    node_1_1.query_and_get_error(
            "INSERT INTO test.test_table "
            "SELECT number AS num FROM numbers(1000,2000) WHERE num % 2 = 0")

    # 3. restore replica
    node_1_1.query("SYSTEM RESTORE REPLICA test.test_table")

    # 4. Check if the data is same on all nodes
    assert zk.exists("/clickhouse/tables/test/1/test_table")
    check_data()

    #5. Check the initial table being attached (not in readonly) and the result being replicated.
    node_1_1.query("INSERT INTO test.test_table SELECT * FROM numbers(1000, 2000)")
    check_data()

    # 6. check we cannot restore the already restored replica
    node_1_1.query_and_get_error("SYSTEM RESTORE REPLICA test.test_table")
