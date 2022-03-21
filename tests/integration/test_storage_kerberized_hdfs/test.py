import time
import pytest

import os

from helpers.cluster import ClickHouseCluster
import subprocess

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_kerberized_hdfs=True, user_configs=[], main_configs=['configs/hdfs.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)
        raise ex
    finally:
        cluster.shutdown()


def test_read_table(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    data = "1\tSerialize\t555.222\n2\tData\t777.333\n"
    hdfs_api.write_data("/simple_table_function", data)

    api_read = hdfs_api.read_data("/simple_table_function")
    assert api_read == data

    select_read = node1.query("select * from hdfs('hdfs://kerberizedhdfs1:9010/simple_table_function', 'TSV', 'id UInt64, text String, number Float64')")
    assert select_read == data

def test_read_write_storage(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query("create table SimpleHDFSStorage2 (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://kerberizedhdfs1:9010/simple_storage1', 'TSV')")
    node1.query("insert into SimpleHDFSStorage2 values (1, 'Mark', 72.53)")

    api_read = hdfs_api.read_data("/simple_storage1")
    assert api_read == "1\tMark\t72.53\n"

    select_read = node1.query("select * from SimpleHDFSStorage2")
    assert select_read == "1\tMark\t72.53\n"

def test_write_storage_not_expired(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query("create table SimpleHDFSStorageNotExpired (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://kerberizedhdfs1:9010/simple_storage_not_expired', 'TSV')")

    time.sleep(15)   # wait for ticket expiration
    node1.query("insert into SimpleHDFSStorageNotExpired values (1, 'Mark', 72.53)")

    api_read = hdfs_api.read_data("/simple_storage_not_expired")
    assert api_read == "1\tMark\t72.53\n"

    select_read = node1.query("select * from SimpleHDFSStorageNotExpired")
    assert select_read == "1\tMark\t72.53\n"

def test_two_users(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    node1.query("create table HDFSStorOne (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://kerberizedhdfs1:9010/storage_user_one', 'TSV')")
    node1.query("insert into HDFSStorOne values (1, 'Real', 86.00)")

    node1.query("create table HDFSStorTwo (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://suser@kerberizedhdfs1:9010/user/specuser/storage_user_two', 'TSV')")
    node1.query("insert into HDFSStorTwo values (1, 'Ideal', 74.00)")

    select_read_1 = node1.query("select * from hdfs('hdfs://kerberizedhdfs1:9010/user/specuser/storage_user_two', 'TSV', 'id UInt64, text String, number Float64')")

    select_read_2 = node1.query("select * from hdfs('hdfs://suser@kerberizedhdfs1:9010/storage_user_one', 'TSV', 'id UInt64, text String, number Float64')")

def test_read_table_expired(started_cluster):
    hdfs_api = started_cluster.hdfs_api

    data = "1\tSerialize\t555.222\n2\tData\t777.333\n"
    hdfs_api.write_data("/simple_table_function_relogin", data)

    started_cluster.pause_container('hdfskerberos')
    time.sleep(15)

    try:
        select_read = node1.query("select * from hdfs('hdfs://reloginuser&kerberizedhdfs1:9010/simple_table_function', 'TSV', 'id UInt64, text String, number Float64')")
        assert False, "Exception have to be thrown"
    except Exception as ex:
        assert "DB::Exception: kinit failure:" in str(ex)

    started_cluster.unpause_container('hdfskerberos')

def test_prohibited(started_cluster):
    node1.query("create table HDFSStorTwoProhibited (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://suser@kerberizedhdfs1:9010/storage_user_two_prohibited', 'TSV')")
    try:
        node1.query("insert into HDFSStorTwoProhibited values (1, 'SomeOne', 74.00)")
        assert False, "Exception have to be thrown"
    except Exception as ex:
        assert "Unable to open HDFS file: /storage_user_two_prohibited error: Permission denied: user=specuser, access=WRITE" in str(ex)

def test_cache_path(started_cluster):
    node1.query("create table HDFSStorCachePath (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://dedicatedcachepath@kerberizedhdfs1:9010/storage_dedicated_cache_path', 'TSV')")
    try:
        node1.query("insert into HDFSStorCachePath values (1, 'FatMark', 92.53)")
        assert False, "Exception have to be thrown"
    except Exception as ex:
        assert "DB::Exception: hadoop.security.kerberos.ticket.cache.path cannot be set per user" in str(ex)


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
