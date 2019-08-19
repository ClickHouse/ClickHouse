import time
import pytest
import requests
from tempfile import NamedTemporaryFile
from helpers.hdfs_api import HDFSApi

import os

from helpers.cluster import ClickHouseCluster
import subprocess


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_hdfs=True, config_dir="configs", main_configs=['configs/log_conf.xml'])

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

def test_read_write_storage(started_cluster):

    hdfs_api = HDFSApi("root")
    hdfs_api.write_data("/simple_storage", "1\tMark\t72.53\n")

    assert hdfs_api.read_data("/simple_storage") == "1\tMark\t72.53\n"

    node1.query("create table SimpleHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/simple_storage', 'TSV')")
    assert node1.query("select * from SimpleHDFSStorage") == "1\tMark\t72.53\n"

def test_read_write_table(started_cluster):
    hdfs_api = HDFSApi("root")
    data = "1\tSerialize\t555.222\n2\tData\t777.333\n"
    hdfs_api.write_data("/simple_table_function", data)

    assert hdfs_api.read_data("/simple_table_function") == data

    assert node1.query("select * from hdfs('hdfs://hdfs1:9000/simple_table_function', 'TSV', 'id UInt64, text String, number Float64')") == data


def test_write_table(started_cluster):
    hdfs_api = HDFSApi("root")

    node1.query("create table OtherHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')")
    node1.query("insert into OtherHDFSStorage values (10, 'tomas', 55.55), (11, 'jack', 32.54)")

    result = "10\ttomas\t55.55\n11\tjack\t32.54\n"
    assert hdfs_api.read_data("/other_storage") == result
    assert node1.query("select * from OtherHDFSStorage order by id") == result

def test_bad_hdfs_uri(started_cluster):
    try:
        node1.query("create table BadStorage1 (id UInt32, name String, weight Float64) ENGINE = HDFS('hads:hgsdfs100500:9000/other_storage', 'TSV')")
    except Exception as ex:
        print ex
        assert 'Illegal HDFS URI' in str(ex)
    try:
        node1.query("create table BadStorage2 (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs100500:9000/other_storage', 'TSV')")
    except Exception as ex:
        print ex
        assert 'Unable to create builder to connect to HDFS' in str(ex)

    try:
        node1.query("create table BadStorage3 (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://hdfs1:9000/<>', 'TSV')")
    except Exception as ex:
        print ex
        assert 'Unable to open HDFS file' in str(ex)

def test_globs_in_read_table(started_cluster):
    hdfs_api = HDFSApi("root")
    some_data = "1\tSerialize\t555.222\n2\tData\t777.333\n"
    globs_dir = "/dir_for_test_with_globs/"
    files = ["simple_table_function", "dir/file", "some_dir/dir1/file", "some_dir/dir2/file", "some_dir/file", "table1_function", "table2_function", "table3_function"]
    for filename in files:
        hdfs_api.write_data(globs_dir + filename, some_data)

    test_requests = [("*_table_functio?", 1),
                     ("dir/fil?", 1),
                     ("table{3..8}_function", 1),
                     ("table{2..8}_function", 2),
                     ("dir/*", 1),
                     ("dir/*?*?*?*?*", 1),
                     ("dir/*?*?*?*?*?*", 0),
                     ("dir/*{a..z}*{a..z}*{a..z}*{a..z}*", 1),
                     ("some_dir/*/file", 2),
                     ("some_dir/dir?/*", 2),
                     ("*/*/*", 2)]

    for pattern, value in test_requests:
        assert node1.query("select * from hdfs('hdfs://hdfs1:9000" + globs_dir + pattern + "', 'TSV', 'id UInt64, text String, number Float64')") == value * some_data