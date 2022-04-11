import time

import helpers.client as client
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", main_configs=["configs/zookeeper_config.xml", "configs/remote_servers.xml"], with_zookeeper=True)
node2 = cluster.add_instance("node2", main_configs=["configs/zookeeper_config.xml", "configs/remote_servers.xml"], with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def drop_table(nodes, table_name):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS {} NO DELAY".format(table_name))

# Create table with default zookeeper.
def test_create_replicated_merge_tree_with_default_zookeeper(started_cluster):
    drop_table([node1, node2], "test_default_zookeeper")
    for node in [node1, node2]:
        node.query(
            '''
                CREATE TABLE test_default_zookeeper(a Int32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_default_zookeeper', '{replica}')
                ORDER BY a;
            '''.format(replica=node.name))

    # Insert data into node1, and query it from node2.
    node1.query("INSERT INTO test_default_zookeeper VALUES (1)")
    time.sleep(5)

    expected = "1\n"
    assert TSV(node1.query("SELECT a FROM test_default_zookeeper")) == TSV(expected)
    assert TSV(node2.query("SELECT a FROM test_default_zookeeper")) == TSV(expected)

# Create table with auxiliary zookeeper.
def test_create_replicated_merge_tree_with_auxiliary_zookeeper(started_cluster):
    drop_table([node1, node2], "test_auxiliary_zookeeper")
    for node in [node1, node2]:
        node.query(
            '''
                CREATE TABLE test_auxiliary_zookeeper(a Int32)
                ENGINE = ReplicatedMergeTree('zookeeper2:/clickhouse/tables/test/test_auxiliary_zookeeper', '{replica}')
                ORDER BY a;
            '''.format(replica=node.name))

    # Insert data into node1, and query it from node2.
    node1.query("INSERT INTO test_auxiliary_zookeeper VALUES (1)")
    time.sleep(5)

    expected = "1\n"
    assert TSV(node1.query("SELECT a FROM test_auxiliary_zookeeper")) == TSV(expected)
    assert TSV(node2.query("SELECT a FROM test_auxiliary_zookeeper")) == TSV(expected)

# Create table with auxiliary zookeeper.
def test_create_replicated_merge_tree_with_not_exists_auxiliary_zookeeper(started_cluster):
    drop_table([node1], "test_auxiliary_zookeeper")
    with pytest.raises(QueryRuntimeException):
        node1.query(
            '''
                CREATE TABLE test_auxiliary_zookeeper(a Int32)
                ENGINE = ReplicatedMergeTree('zookeeper_not_exits:/clickhouse/tables/test/test_auxiliary_zookeeper', '{replica}')
                ORDER BY a;
            '''.format(replica=node1.name))

# Drop table with auxiliary zookeeper.
def test_drop_replicated_merge_tree_with_auxiliary_zookeeper(started_cluster):
    drop_table([node1, node2], "test_auxiliary_zookeeper")
    for node in [node1, node2]:
        node.query(
            '''
                CREATE TABLE test_auxiliary_zookeeper(a Int32)
                ENGINE = ReplicatedMergeTree('zookeeper2:/clickhouse/tables/test/test_auxiliary_zookeeper', '{replica}')
                ORDER BY a;
            '''.format(replica=node.name))

    # Insert data into node1, and query it from node2.
    node1.query("INSERT INTO test_auxiliary_zookeeper VALUES (1)")
    time.sleep(5)

    expected = "1\n"
    assert TSV(node1.query("SELECT a FROM test_auxiliary_zookeeper")) == TSV(expected)
    assert TSV(node2.query("SELECT a FROM test_auxiliary_zookeeper")) == TSV(expected)

    zk = cluster.get_kazoo_client('zoo1')
    assert zk.exists('/clickhouse/tables/test/test_auxiliary_zookeeper')
    drop_table([node1, node2], "test_auxiliary_zookeeper")
    assert zk.exists('/clickhouse/tables/test/test_auxiliary_zookeeper') is None

def test_path_ambiguity(started_cluster):
    drop_table([node1, node2], "test_path_ambiguity1")
    drop_table([node1, node2], "test_path_ambiguity2")
    node1.query("create table test_path_ambiguity1 (n int) engine=ReplicatedMergeTree('/test:bad:/path', '1') order by n")
    assert "Invalid auxiliary ZooKeeper name" in node1.query_and_get_error("create table test_path_ambiguity2 (n int) engine=ReplicatedMergeTree('test:bad:/path', '1') order by n")
    assert "ZooKeeper path must starts with '/'" in node1.query_and_get_error("create table test_path_ambiguity2 (n int) engine=ReplicatedMergeTree('test/bad:/path', '1') order by n")
    node1.query("create table test_path_ambiguity2 (n int) engine=ReplicatedMergeTree('zookeeper2:/bad:/path', '1') order by n")
    drop_table([node1, node2], "test_path_ambiguity1")
    drop_table([node1, node2], "test_path_ambiguity2")
