import time
import contextlib
import pymysql.cursors
import pytest
import os
import subprocess

from helpers.client import QueryRuntimeException
from helpers.mysql_helpers import MySQLNodeInstance
from helpers.cluster import ClickHouseCluster, get_docker_compose_path
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
clickhouse_node = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_mysql=True, stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_disabled_mysql_server(started_cluster):
    with contextlib.closing(MySQLNodeInstance()) as mysql_node:
        mysql_node.query("CREATE DATABASE test_db;")
        mysql_node.query("CREATE TABLE test_db.test_table ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")

    with PartitionManager() as pm:
        with pytest.raises(QueryRuntimeException) as exception:
            pm._add_rule({'source': clickhouse_node.ip_address, 'destination_port': 3306, 'action': 'DROP'})
            clickhouse_node.query("CREATE DATABASE test_db ENGINE = MySQL('mysql1:3306', 'test_db', 'root', 'clickhouse')")

        assert "MySQL database server is unavailable" in str(exception.value)
        pm._delete_rule({'source': clickhouse_node.ip_address, 'destination_port': 3306, 'action': 'DROP'})

        clickhouse_node.query("CREATE DATABASE test_db ENGINE = MySQL('mysql1:3306', 'test_db', 'root', 'clickhouse')")
        assert 'test_table' in clickhouse_node.query("SHOW TABLES FROM test_db")

        pm._add_rule({'source': clickhouse_node.ip_address, 'destination_port': 3306, 'action': 'DROP'})
        clickhouse_node.restart_clickhouse(stop_start_wait_sec=10)  # successfully
        with pytest.raises(QueryRuntimeException) as exception:
            clickhouse_node.query("SHOW TABLES FORM test_db")
        assert "MySQL database server is unavailable" in str(exception.value)

        clickhouse_node.query("SELECT * FROM system.parts")
        clickhouse_node.query("SELECT * FROM system.mutations")
        clickhouse_node.query("SELECT * FROM system.graphite_retentions")

        clickhouse_node.query("DROP DATABASE test_db")  # successfully
