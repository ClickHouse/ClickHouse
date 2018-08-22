import time
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__, server_bin_path="/home/alesap/ClickHouse/dbms/programs/clickhouse")
node1 = cluster.add_instance('node1', with_odbc_drivers=True, with_mysql=True, image='alesapin/ubuntu_with_odbc:14.04')

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()

def test_segfault_doesnt_crash_server(started_cluster):
    mysql_setup = node1.odbc_drivers["MySQL"]
    # actually, I don't know, what wrong with that connection string, but libmyodbc always falls into segfault
    node1.query("select 1 from odbc('DSN={}', 'dual')".format(mysql_setup["DSN"]), ignore_error=True)

    # but after segfault server is still available
    assert node1.query("select 1") == "1\n"

def test_simple_select_works(started_cluster):
    sqlite_setup = node1.odbc_drivers["SQLite3"]
    sqlite_db = sqlite_setup["Database"]

    node1.exec_in_container(["bash", "-c", "echo 'CREATE TABLE t1(x INTEGER PRIMARY KEY ASC, y, z);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')
    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t1 values(1, 2, 3);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')
    assert node1.query("select * from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "1\t2\t3\n"


