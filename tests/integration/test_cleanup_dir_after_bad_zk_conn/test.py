import time

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# This tests if the data directory for a table is cleaned up if there is a Zookeeper
# connection exception during a CreateQuery operation involving ReplicatedMergeTree tables.
# Test flow is as follows:
# 1. Configure cluster with ZooKeeper and create a database.
# 2. Drop all connections to ZooKeeper.
# 3. Try creating the table and there will be a Poco:Exception.
# 4. Try creating the table again and there should not be any error
# that indicates that the directory for table already exists.
# 5. Final step is to restore ZooKeeper connection and verify that
# the table creation works.
def test_cleanup_dir_after_bad_zk_conn(start_cluster):
    node1.query("CREATE DATABASE replica;")
    query_create = '''CREATE TABLE replica.test
    (
       id Int64,
       event_time DateTime
    )
    Engine=ReplicatedMergeTree('/clickhouse/tables/replica/test', 'node1')
    PARTITION BY toYYYYMMDD(event_time)
    ORDER BY id;'''
    with PartitionManager() as pm:
        pm.drop_instance_zk_connections(node1)
        time.sleep(3)
        error = node1.query_and_get_error(query_create)
        assert "Poco::Exception. Code: 1000" and \
               "All connection tries failed while connecting to ZooKeeper" in error
        error = node1.query_and_get_error(query_create)
        assert "Directory for table data data/replica/test/ already exists" not in error
    node1.query(query_create)
    node1.query('''INSERT INTO replica.test VALUES (1, now())''')
    assert "1\n" in node1.query('''SELECT count() from replica.test FORMAT TSV''')


def test_cleanup_dir_after_wrong_replica_name(start_cluster):
    node1.query(
        "CREATE TABLE test2_r1 (n UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test2/', 'r1') ORDER BY n")
    error = node1.query_and_get_error(
        "CREATE TABLE test2_r2 (n UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test2/', 'r1') ORDER BY n")
    assert "already exists" in error
    node1.query(
        "CREATE TABLE test_r2 (n UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test2/', 'r2') ORDER BY n")


def test_cleanup_dir_after_wrong_zk_path(start_cluster):
    node1.query(
        "CREATE TABLE test3_r1 (n UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test3/', 'r1') ORDER BY n")
    error = node1.query_and_get_error(
        "CREATE TABLE test3_r2 (n UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/', 'r2') ORDER BY n")
    assert "Cannot create" in error
    node1.query(
        "CREATE TABLE test3_r2 (n UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test3/', 'r2') ORDER BY n")


def test_attach_without_zk(start_cluster):
    node1.query(
        "CREATE TABLE test4_r1 (n UInt64) ENGINE=ReplicatedMergeTree('/clickhouse/tables/test4/', 'r1') ORDER BY n")
    node1.query("DETACH TABLE test4_r1")
    with PartitionManager() as pm:
        pm._add_rule({'probability': 0.5, 'source': node1.ip_address, 'destination_port': 2181, 'action': 'DROP'})
        try:
            node1.query("ATTACH TABLE test4_r1")
        except:
            pass
    node1.query("ATTACH TABLE IF NOT EXISTS test4_r1")
    node1.query("SELECT * FROM test4_r1")
