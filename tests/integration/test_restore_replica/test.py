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

query_steps_log = [
    "Started restoring",
    "Created a new replicated table",
    "Stopped replica fetches for",
    "Moved and attached all parts from",
    "Renamed tables",
    "Detached old table",
    "Removed old table"
]

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


def check_data(_sum: int, count: int):
    res: str = node_1.query("SELECT sum(n), count() FROM test")
    assert_eq_with_retry(node_2, "SELECT sum(n), count() FROM test", res)
    assert_eq_with_retry(node_3, "SELECT sum(n), count() FROM test", res)

    test_sum, test_count = map(int, res.split())
    assert test_count == count
    assert test_sum == _sum


def test_restore_replica(start_cluster):
    zk = cluster.get_kazoo_client('zoo1')

    print("Checking the invocation on non-existent and non-replicated tables")
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA i_dont_exist_42")
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA no_db.i_dont_exist_42")
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA system.numbers")

    check_data(0, 0)
    node_1.query("INSERT INTO test SELECT * FROM numbers(1000)")
    check_data(499500, 1000)

    # 1. Delete individual replicas paths in ZK and check the invocation
    print("Deleting replica2 path, trying to restore replica1")
    zk.delete("/clickhouse/tables/test/replicas/replica2", recursive=True)
    assert zk.exists("/clickhouse/tables/test/replicas/replica2") is None
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA test")

    print("Deleting replica1 path, trying to restore replica1")
    zk.delete("/clickhouse/tables/test/replicas/replica1", recursive=True)
    assert zk.exists("/clickhouse/tables/test/replicas/replica1") is None

    node_1.query("SYSTEM RESTART REPLICA test") # will restore the table
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA test")
    node_2.query("SYSTEM RESTART REPLICA test")
    node_2.query_and_get_error("SYSTEM RESTORE REPLICA test")

    check_data(499500, 1000)

    node_1.query("INSERT INTO test SELECT number + 1000 FROM numbers(1000)") # assert all the tables are working

    node_2.query("SYSTEM SYNC REPLICA test")
    node_3.query("SYSTEM SYNC REPLICA test")
    check_data(1999000, 2000)

    # 2. Delete metadata for the root zk path (emulating a Zookeeper error)
    print("Deleting root ZK path metadata")
    zk.delete("/clickhouse/tables/test", recursive=True)
    assert zk.exists("/clickhouse/tables/test") is None

    node_1.query("SYSTEM RESTART REPLICA test")
    # 3. Assert the table is readonly as the metadata is missing
    node_1.query_and_get_error("INSERT INTO test SELECT number AS num FROM numbers(1000,2000) WHERE num % 2 = 0")

    print("Restoring replica1")
    node_1.query("SYSTEM RESTORE REPLICA test")
    assert zk.exists("/clickhouse/tables/test")
    check_data(1999000, 2000)

    node_1.query("INSERT INTO test SELECT number + 2000 FROM numbers(1000)")

    print("Restoring other replicas")
    node_1.query("SYSTEM RESTART REPLICA test") # will restore the table
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA test")
    node_2.query("SYSTEM RESTART REPLICA test")
    node_2.query_and_get_error("SYSTEM RESTORE REPLICA test")

    check_data(4498500, 3000)

    # 7. check we cannot restore the already restored replica
    node_1.query_and_get_error("SYSTEM RESTORE REPLICA test")
