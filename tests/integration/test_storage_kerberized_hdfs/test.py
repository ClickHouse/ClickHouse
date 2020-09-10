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
node1 = cluster.add_instance('node1', with_kerberized_hdfs=True, user_configs=[], main_configs=['configs/log_conf.xml', 'configs/hdfs.xml'])

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

    # node1.query("create table SimpleHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://kerberized_hdfs1.test.clickhouse.tech:9000/simple_storage', 'TSV')")
    node1.query("create table SimpleHDFSStorage (id UInt32, name String, weight Float64) ENGINE = HDFS('hdfs://kerberizedhdfs1:9000/simple_storage', 'TSV')")
    node1.query("insert into SimpleHDFSStorage values (1, 'Mark', 72.53)")
    assert hdfs_api.read_data("/simple_storage") == "1\tMark\t72.53\n"
    assert node1.query("select * from SimpleHDFSStorage") == "1\tMark\t72.53\n"


if __name__ == '__main__':
    cluster.start()
    raw_input("Cluster created, press any key to destroy...")
    cluster.shutdown()
