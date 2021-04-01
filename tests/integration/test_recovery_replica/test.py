import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


def fill_nodes(nodes, shard):
    for node in nodes:
        node.query(
            '''
                CREATE TABLE test_table(date Date, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/replicated', '{replica}') ORDER BY id PARTITION BY toYYYYMM(date) SETTINGS min_replicated_logs_to_keep=3, max_replicated_logs_to_keep=5, cleanup_delay_period=0, cleanup_delay_period_random_add=0;
            '''.format(shard=shard, replica=node.name))


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)
node3 = cluster.add_instance('node3', with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        fill_nodes([node1, node2, node3], 1)

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def test_recovery(start_cluster):
    node1.query("INSERT INTO test_table VALUES (1, 1)")
    time.sleep(1)
    node2.query("DETACH TABLE test_table")

    for i in range(100):
        node1.query("INSERT INTO test_table VALUES (1, {})".format(i))

    node2.query_with_retry("ATTACH TABLE test_table",
                           check_callback=lambda x: len(node2.query("select * from test_table")) > 0)

    assert_eq_with_retry(node2, "SELECT count(*) FROM test_table", node1.query("SELECT count(*) FROM test_table"))
    lost_marker = "Will mark replica node2 as lost"
    assert node1.contains_in_log(lost_marker) or node3.contains_in_log(lost_marker)

def test_choose_source_replica(start_cluster):
    node3.query("INSERT INTO test_table VALUES (2, 1)")
    time.sleep(1)
    node2.query("DETACH TABLE test_table")
    node1.query("SYSTEM STOP FETCHES test_table")   # node1 will have many entries in queue, so node2 will clone node3

    for i in range(100):
        node3.query("INSERT INTO test_table VALUES (2, {})".format(i))

    node2.query_with_retry("ATTACH TABLE test_table",
                           check_callback=lambda x: len(node2.query("select * from test_table")) > 0)

    node1.query("SYSTEM START FETCHES test_table")
    node1.query("SYSTEM SYNC REPLICA test_table")
    node2.query("SYSTEM SYNC REPLICA test_table")

    assert node1.query("SELECT count(*) FROM test_table") == node3.query("SELECT count(*) FROM test_table")
    assert node2.query("SELECT count(*) FROM test_table") == node3.query("SELECT count(*) FROM test_table")

    lost_marker = "Will mark replica node2 as lost"
    assert node1.contains_in_log(lost_marker) or node3.contains_in_log(lost_marker)
    assert node2.contains_in_log("Will mimic node3")

