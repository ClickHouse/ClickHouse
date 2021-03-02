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
            CREATE TABLE test(n UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
            ORDER BY n PARTITION BY n % 10;
        '''.format(replica=node.name))

cluster = ClickHouseCluster(__file__)
configs =["configs/remote_servers.xml"]

node_1 = cluster.add_instance('replica1', with_zookeeper=True, main_configs=configs)
node_2 = cluster.add_instance('replica2', with_zookeeper=True, main_configs=configs)
node_3 = cluster.add_instance('replica3', with_zookeeper=True, main_configs=configs)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        fill_nodes([node_1, node_2, node_3])
        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def check_data():
    res: str = node_1.query("SELECT sum(n), count(n) FROM test")
    assert_eq_with_retry(node_2, "SELECT sum(n), count(n) FROM test", res)
    assert_eq_with_retry(node_3, "SELECT sum(n), count(n) FROM test", res)


def test_restore_replica(start_cluster):
    zk = cluster.get_kazoo_client('zoo1')

    # 0. Check the invocation on non-existent and non-replicated tables
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA i_dont_exist_42")
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA no_db.i_dont_exist_42")
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA system.numbers")

    # 0. Assert all the replicas have the inserted data and output debug info
    node_1.query("INSERT INTO test SELECT * FROM numbers(1000, 2000)")
    check_data()

    # 1. Delete individual replicas paths in ZK and check that the restoration query will return an error
    # (there's nothing to restore as long as there is a single replica path in ZK)

    zk.delete("/clickhouse/tables/test/replicas/replica1", recursive=True)
    assert zk.exists("/clickhouse/tables/test/replicas/replica1") is None

    node_1.query_and_get_error("SYSTEM RESTORE REPLICA test")

    zk.delete("/clickhouse/tables/test/replicas/replica2", recursive=True)
    assert zk.exists("/clickhouse/tables/test/replicas/replica2") is None

    node_1.query_and_get_error("SYSTEM RESTORE REPLICA test")

    # 2. Delete metadata for the root zk path (emulating a Zookeeper error)
    zk.delete("/clickhouse/tables/test/1/test_table", recursive=True)
    assert zk.exists("/clickhouse/tables/test/1/test_table") is None

    node_1.query("SYSTEM RESTART REPLICA test")

    # 3. Assert there is an exception as the metadata is missing
    node_1.query_and_get_error("INSERT INTO test SELECT number AS num FROM numbers(1000,2000) WHERE num % 2 = 0")

    # 4. restore replica
    node_1.query("SYSTEM RESTORE REPLICA test")

    # 5. Check if the data is same on all nodes
    assert zk.exists("/clickhouse/tables/test/")
    check_data()

    # 6. Check the initial table being attached (not in readonly) and the result being replicated.
    node_1.query("INSERT INTO test SELECT * FROM numbers(1000, 2000)")
    check_data()

    # 7. check we cannot restore the already restored replica
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA test")
