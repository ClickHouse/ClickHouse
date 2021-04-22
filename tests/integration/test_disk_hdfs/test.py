import os
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.hdfs_api import HDFSApi

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=[
    'configs/storage.xml',
    'configs/log_conf.xml'], with_hdfs=True)

node2 = cluster.add_instance('node2', main_configs=[
    'configs/storage_hdfs_as_default.xml',
    'configs/log_conf.xml'], with_hdfs=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_read_write(started_cluster):
    node1.query("DROP TABLE IF EXISTS simple_test")
    node1.query("CREATE TABLE simple_test (id UInt64) Engine=TinyLog SETTINGS disk = 'hdfs'")
    node1.query("INSERT INTO simple_test SELECT number FROM numbers(3)")
    node1.query("INSERT INTO simple_test SELECT number FROM numbers(3, 3)")
    assert node1.query("SELECT * FROM simple_test") == "0\n1\n2\n3\n4\n5\n"


def test_hdfs_disk_as_default(started_cluster):
    assert int(node2.query("SELECT count() FROM system.disks")) == 1
    assert node2.query("SELECT name, type FROM system.disks") == "default\thdfs\n"

    node2.query("CREATE DATABASE test_database")
    assert 'test_database' in node2.query('SHOW DATABASES')

    node2.query("DROP TABLE IF EXISTS test_database.test_table")
    assert 'test_table' not in node2.query('SHOW TABLES FROM test_database')

    node2.query("CREATE TABLE test_database.test_table (id UInt32) Engine=Memory()")
    assert 'test_table' in node2.query('SHOW TABLES FROM test_database')

    for i in range(5):
        node2.query("INSERT INTO test_database.test_table SELECT number FROM numbers(100000)")
    assert int(node2.query("SELECT count() FROM test_database.test_table").rstrip()) == 5 * 100000

    node2.query("RENAME TABLE test_database.test_table to test_database.test")
    assert int(node2.query("SELECT count() FROM test_database.test").rstrip()) == 5 * 100000

    node2.query("RENAME TABLE test_database.test to test_database.test_table")

    node2.query("TRUNCATE TABLE test_database.test_table")
    assert int(node2.query("SELECT count() FROM test_database.test_table").rstrip()) == 0

    node2.query("INSERT INTO test_database.test_table SELECT number FROM numbers(100000)")
    assert int(node2.query("SELECT count() FROM test_database.test_table").rstrip()) == 100000

    node2.query("DETACH TABLE test_database.test_table")
    assert 'test_table' not in node2.query('SHOW TABLES FROM test_database')

    node2.query("ATTACH TABLE test_database.test_table")
    assert 'test_table' in node2.query('SHOW TABLES FROM test_database')

    node2.query("DROP TABLE test_database.test_table")
    assert 'test_table' not in node2.query('SHOW TABLES FROM test_database')

    node2.query("DROP DATABASE test_database")
    assert 'test_database' not in node2.query('SHOW DATABASES')
