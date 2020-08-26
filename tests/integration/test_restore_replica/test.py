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
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/replicated/test_table', '{replica}')
            ORDER BY num
            PARTITION BY intDiv(num % 1000);
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

    node_1_1.query()
    node_1_1.query("INSERT INTO test.test_table SELECT * FROM numbers(1000, 2000)")

    with PartitionManager() as pm:
        ## make node_1_1 dead
        pm.drop_instance_zk_connections(node_1_1)
        time.sleep(10)

        assert "doesn't exist" in node_1_3.query_and_get_error("SYSTEM DROP REPLICA 'node_1_1' FROM TABLE test.test_table")

        assert "doesn't exist" in node_1_3.query_and_get_error("SYSTEM DROP REPLICA 'node_1_1' FROM DATABASE test1")

        node_1_3.query("SYSTEM DROP REPLICA 'node_1_1'")
        exists_replica_1_1 = zk.exists("/clickhouse/tables/test3/{shard}/replicated/test_table/replicas/{replica}".format(shard=1, replica='node_1_1'))
        assert (exists_replica_1_1 != None)

        ## If you want to drop a inactive/stale replicate table that does not have a local replica, you can following syntax(ZKPATH):
        node_1_3.query("SYSTEM DROP REPLICA 'node_1_1' FROM ZKPATH '/clickhouse/tables/test2/{shard}/replicated/test_table'".format(shard=1))
        exists_replica_1_1 = zk.exists("/clickhouse/tables/test2/{shard}/replicated/test_table/replicas/{replica}".format(shard=1, replica='node_1_1'))
        assert (exists_replica_1_1 == None)

        node_1_2.query("SYSTEM DROP REPLICA 'node_1_1' FROM TABLE test.test_table")
        exists_replica_1_1 = zk.exists("/clickhouse/tables/test/{shard}/replicated/test_table/replicas/{replica}".format(shard=1, replica='node_1_1'))
        assert (exists_replica_1_1 == None)

        node_1_2.query("SYSTEM DROP REPLICA 'node_1_1' FROM DATABASE test1")
        exists_replica_1_1 = zk.exists("/clickhouse/tables/test1/{shard}/replicated/test_table/replicas/{replica}".format(shard=1, replica='node_1_1'))
        assert (exists_replica_1_1 == None)

        node_1_3.query("SYSTEM DROP REPLICA 'node_1_1' FROM ZKPATH '/clickhouse/tables/test3/{shard}/replicated/test_table'".format(shard=1))
        exists_replica_1_1 = zk.exists("/clickhouse/tables/test3/{shard}/replicated/test_table/replicas/{replica}".format(shard=1, replica='node_1_1'))
        assert (exists_replica_1_1 == None)

        node_1_2.query("SYSTEM DROP REPLICA 'node_1_1'")
        exists_replica_1_1 = zk.exists("/clickhouse/tables/test4/{shard}/replicated/test_table/replicas/{replica}".format(shard=1, replica='node_1_1'))
        assert (exists_replica_1_1 == None)
