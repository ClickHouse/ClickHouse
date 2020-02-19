import time
import pytest

import helpers.client as client
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
        node.query("DROP TABLE IF EXISTS {}".format(table_name))

def test_ttl_columns(started_cluster):
    drop_table([node1, node2], "test_ttl")
    for node in [node1, node2]:
        node.query(
        '''
            CREATE TABLE test_ttl(date DateTime, id UInt32, a Int32 TTL date + INTERVAL 1 DAY, b Int32 TTL date + INTERVAL 1 MONTH)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl', '{replica}')
            ORDER BY id PARTITION BY toDayOfMonth(date) SETTINGS merge_with_ttl_timeout=0;
        '''.format(replica=node.name))

    node1.query("INSERT INTO test_ttl VALUES (toDateTime('2000-10-10 00:00:00'), 1, 1, 3)")
    node1.query("INSERT INTO test_ttl VALUES (toDateTime('2000-10-11 10:00:00'), 2, 2, 4)")
    time.sleep(1) # sleep to allow use ttl merge selector for second time
    node1.query("OPTIMIZE TABLE test_ttl FINAL")

    expected = "1\t0\t0\n2\t0\t0\n"
    assert TSV(node1.query("SELECT id, a, b FROM test_ttl ORDER BY id")) == TSV(expected)
    assert TSV(node2.query("SELECT id, a, b FROM test_ttl ORDER BY id")) == TSV(expected)
 
@pytest.mark.parametrize("delete_suffix", [
    "",
    "DELETE",
])
def test_ttl_table(started_cluster, delete_suffix):
    drop_table([node1, node2], "test_ttl")
    for node in [node1, node2]:
        node.query(
        '''
            CREATE TABLE test_ttl(date DateTime, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl', '{replica}')
            ORDER BY id PARTITION BY toDayOfMonth(date)
            TTL date + INTERVAL 1 DAY {delete_suffix} SETTINGS merge_with_ttl_timeout=0;
        '''.format(replica=node.name, delete_suffix=delete_suffix))

    node1.query("INSERT INTO test_ttl VALUES (toDateTime('2000-10-10 00:00:00'), 1)")
    node1.query("INSERT INTO test_ttl VALUES (toDateTime('2000-10-11 10:00:00'), 2)")
    time.sleep(1) # sleep to allow use ttl merge selector for second time
    node1.query("OPTIMIZE TABLE test_ttl FINAL")

    assert TSV(node1.query("SELECT * FROM test_ttl")) == TSV("")
    assert TSV(node2.query("SELECT * FROM test_ttl")) == TSV("")

def test_ttl_double_delete_rule_returns_error(started_cluster):
    drop_table([node1, node2], "test_ttl")
    try:
        node1.query('''
            CREATE TABLE test_ttl(date DateTime, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl', '{replica}')
            ORDER BY id PARTITION BY toDayOfMonth(date)
            TTL date + INTERVAL 1 DAY, date + INTERVAL 2 DAY SETTINGS merge_with_ttl_timeout=0;
        '''.format(replica=node1.name))
        assert False
    except client.QueryRuntimeException:
        pass
    except:
        assert False
