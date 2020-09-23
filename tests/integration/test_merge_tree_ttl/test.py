import time

import helpers.client as client
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print ex

    finally:
        cluster.shutdown()


def drop_table(nodes, table_name):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS {} NO DELAY".format(table_name))
    time.sleep(1)

def test_ttl_columns_for_replicated_merge_tree(started_cluster):
    drop_table([node1, node2], "test_ttl")
    for node in [node1, node2]:
        node.query(
            '''
                CREATE TABLE test_ttl(c String, when DateTime)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl', '{replica}')
                PARTITION BY toYYYYMM(when)
                ORDER BY c
                TTL when + INTERVAL 10 second
                SETTINGS ttl_only_drop_parts=1;
            '''.format(replica=node.name))

    node1.query("INSERT INTO test_ttl VALUES ('a', now())")
    node1.query("INSERT INTO test_ttl VALUES ('b', now())")
    time.sleep(15)  # sleep to allow rows to be expired.
    node1.query("OPTIMIZE TABLE test_ttl FINAL")
    time.sleep(5)

    expected = "0\n"
    assert TSV(node1.query("SELECT count() FROM test_ttl")) == TSV(expected)
    assert TSV(node2.query("SELECT count() FROM test_ttl")) == TSV(expected)

    assert TSV(node1.query("SELECT count() FROM system.parts WHERE table = 'test_ttl' AND active = 1")) == TSV(expected)
    assert TSV(node2.query("SELECT count() FROM system.parts WHERE table = 'test_ttl' AND active = 1")) == TSV(expected)

def test_ttl_columns_for_merge_tree(started_cluster):
    drop_table([node1], "test_ttl_2")
    node1.query(
        '''
            CREATE TABLE test_ttl(c String, when DateTime)
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(when)
            ORDER BY c
            TTL when + INTERVAL 10 second
            SETTINGS ttl_only_drop_parts=1;
        '''.format(replica=node.name))

    node1.query("INSERT INTO test_ttl VALUES ('a', now())")
    time.sleep(15)  # sleep to allow rows to be expired.
    node1.query("OPTIMIZE TABLE test_ttl FINAL")
    time.sleep(5)

    expected = "0\n"
    assert TSV(node1.query("SELECT count() FROM test_ttl_2")) == TSV(expected)

    assert TSV(node1.query("SELECT count() FROM system.parts WHERE table = 'test_ttl_2' AND active = 1")) == TSV(expected)
