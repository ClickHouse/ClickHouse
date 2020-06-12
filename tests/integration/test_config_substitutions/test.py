import time
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', user_configs=['configs/config_no_substs.xml']) # hardcoded value 33333
node2 = cluster.add_instance('node2', user_configs=['configs/config_env.xml'], env_variables={"MAX_QUERY_SIZE": "55555"})
node3 = cluster.add_instance('node3', user_configs=['configs/config_zk.xml'], with_zookeeper=True)
node4 = cluster.add_instance('node4', user_configs=['configs/config_incl.xml'], main_configs=['configs/max_query_size.xml']) # include value 77777
node5 = cluster.add_instance('node5', user_configs=['configs/config_allow_databases.xml'])
node6 = cluster.add_instance('node6', user_configs=['configs/config_include_from_env.xml'], env_variables={"INCLUDE_FROM_ENV": "/etc/clickhouse-server/config.d/max_query_size.xml"}, main_configs=['configs/max_query_size.xml'])

@pytest.fixture(scope="module")
def start_cluster():
    try:
        def create_zk_roots(zk):
            zk.create(path="/setting/max_query_size", value="77777", makepath=True)
        cluster.add_zookeeper_startup_command(create_zk_roots)

        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

def test_config(start_cluster):
   assert node1.query("select value from system.settings where name = 'max_query_size'") == "33333\n"
   assert node2.query("select value from system.settings where name = 'max_query_size'") == "55555\n"
   assert node3.query("select value from system.settings where name = 'max_query_size'") == "77777\n"
   assert node4.query("select value from system.settings where name = 'max_query_size'") == "99999\n"
   assert node6.query("select value from system.settings where name = 'max_query_size'") == "99999\n"

def test_allow_databases(start_cluster):
    node5.query("CREATE DATABASE db1")
    node5.query("CREATE TABLE db1.test_table(date Date, k1 String, v1 Int32) ENGINE = MergeTree(date, (k1, date), 8192)")
    node5.query("INSERT INTO db1.test_table VALUES('2000-01-01', 'test_key', 1)")
    assert node5.query("SELECT name FROM system.databases WHERE name = 'db1'") == "db1\n"
    assert node5.query("SELECT name FROM system.tables WHERE database = 'db1' AND name = 'test_table' ") == "test_table\n"
    assert node5.query("SELECT name FROM system.columns WHERE database = 'db1' AND table = 'test_table'") == "date\nk1\nv1\n"
    assert node5.query("SELECT name FROM system.parts WHERE database = 'db1' AND table = 'test_table'") == "20000101_20000101_1_1_0\n"
    assert node5.query("SELECT name FROM system.parts_columns WHERE database = 'db1' AND table = 'test_table'") == "20000101_20000101_1_1_0\n20000101_20000101_1_1_0\n20000101_20000101_1_1_0\n"

    assert node5.query("SELECT name FROM system.databases WHERE name = 'db1'", user="test_allow").strip() == ""
    assert node5.query("SELECT name FROM system.tables WHERE database = 'db1' AND name = 'test_table'", user="test_allow").strip() == ""
    assert node5.query("SELECT name FROM system.columns WHERE database = 'db1' AND table = 'test_table'", user="test_allow").strip() == ""
    assert node5.query("SELECT name FROM system.parts WHERE database = 'db1' AND table = 'test_table'", user="test_allow").strip() == ""
    assert node5.query("SELECT name FROM system.parts_columns WHERE database = 'db1' AND table = 'test_table'", user="test_allow").strip() == ""
