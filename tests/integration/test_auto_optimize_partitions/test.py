import pytest
import time;
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/zookeeper_config.xml", "configs/merge_tree.xml"], with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()

def test_without_auto_optimize_merge_tree(start_cluster):
    node.query(
        "CREATE TABLE test (i Int64) ENGINE = MergeTree ORDER BY i;"
    )
    node.query("INSERT INTO test SELECT 1")
    node.query("INSERT INTO test SELECT 2")
    node.query("INSERT INTO test SELECT 3")


    time.sleep(5)

    expected = TSV('''3\n''')
    assert TSV(node.query("SELECT count(*) FROM system.parts where table='test' and active=1")) == expected

    node.query("DROP TABLE test;")

def test_auto_optimize_merge_tree(start_cluster):
    node.query(
        "CREATE TABLE test (i Int64) ENGINE = MergeTree ORDER BY i SETTINGS auto_optimize_partition_after_seconds=5;"
    )
    node.query("INSERT INTO test SELECT 1")
    node.query("INSERT INTO test SELECT 2")
    node.query("INSERT INTO test SELECT 3")


    time.sleep(10)

    expected = TSV('''1\n''')
    assert TSV(node.query("SELECT count(*) FROM system.parts where table='test' and active=1")) == expected

    node.query("DROP TABLE test;")

def test_auto_optimize_replicated_merge_tree(start_cluster):
    node.query(
        "CREATE TABLE test (i Int64) ENGINE = ReplicatedMergeTree('/clickhouse/testing/test', 'node') ORDER BY i SETTINGS auto_optimize_partition_after_seconds=5;"
    )
    node.query("INSERT INTO test SELECT 1")
    node.query("INSERT INTO test SELECT 2")
    node.query("INSERT INTO test SELECT 3")


    time.sleep(10)

    expected = TSV('''1\n''')
    assert TSV(node.query("SELECT count(*) FROM system.parts where table='test' and active=1")) == expected

    node.query("DROP TABLE test;")
