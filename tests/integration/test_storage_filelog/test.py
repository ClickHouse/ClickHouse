import pytest
import time
import shutil
import os
from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1',
                             main_configs=['configs/user_files_path.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_select(started_cluster):
    try:
        name = "filelog_table"
        node1.query("""
            CREATE TABLE {name} (id UInt64, value UInt64) Engine=FileLog('logs', 'CSV')
        """.format(name=name))

        assert node1.query("SELECT sum(id) FROM {name}".format(name=name)) == "55\n"

        shutil.copy("user_files/logs/data.csv", "user_files/logs/data_copy.csv")

        assert node1.query("SELECT sum(id) FROM {name}".format(name=name)) == "55\n"

    finally:
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))
        os.remove("user_files/logs/data_copy.csv")


def test_stream_to_view(started_cluster):
    try:
        name = "filelog_table"
        node1.query("""
            CREATE TABLE {name} (id UInt64, value UInt64) Engine=FileLog('logs', 'CSV')
        """.format(name=name))

        target_name = "mv"
        node1.query("""
            CREATE  Materialized View {target_name} engine=MergeTree order by id as select * from {name}
        """.format(target_name = target_name, name=name))

        time.sleep(10)

        assert node1.query("SELECT sum(id) FROM {target_name}".format(target_name=target_name)) == "55\n"

        shutil.copy("user_files/logs/data.csv" "user_files/logs/data_copy.csv")

        time.sleep(10)

        assert node1.query("SELECT sum(id) FROM {target_name}".format(target_name=target_name)) == "110\n"

    finally:
        node1.query("DROP TABLE IF EXISTS {target_name}".format(target_name=target_name))
        node1.query("DROP TABLE IF EXISTS {name}".format(name=name))
        os.remove("user_files/logs/data_copy.csv")
