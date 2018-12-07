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
node1 = cluster.add_instance('node1', with_hdfs=True, image='withlibsimage', config_dir="configs", main_configs=['configs/log_conf.xml'])

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
