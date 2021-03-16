import time
import contextlib
import pymysql.cursors
import pytest
import os
import subprocess

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, get_docker_compose_path
from helpers.network import PartitionManager

cluster = ClickHouseCluster(__file__)
clickhouse_node = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_mysql=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


class MySQLNodeInstance:
    def __init__(self, started_cluster, user='root', password='clickhouse'):
        self.user = user
        self.port = cluster.mysql_port
        self.hostname = cluster.mysql_ip
        self.password = password
        self.mysql_connection = None   # lazy init

    def alloc_connection(self):
        if self.mysql_connection is None:
            self.mysql_connection = pymysql.connect(user=self.user, password=self.password, host=self.hostname,
                                                    port=self.port, autocommit=True)
        return self.mysql_connection

    def query(self, execution_query):
        with self.alloc_connection().cursor() as cursor:
            cursor.execute(execution_query)

    def close(self):
        if self.mysql_connection is not None:
            self.mysql_connection.close()


def test_disabled_mysql_server(started_cluster):
    with contextlib.closing(MySQLNodeInstance(started_cluster)) as mysql_node:
        mysql_node.query("DROP DATABASE IF EXISTS test_db_disabled;")
        mysql_node.query("CREATE DATABASE test_db_disabled;")
        mysql_node.query("CREATE TABLE test_db_disabled.test_table ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")

    with PartitionManager() as pm:
        clickhouse_node.query("CREATE DATABASE test_db_disabled ENGINE = MySQL('mysql57:3306', 'test_db_disabled', 'root', 'clickhouse')")
            
        pm._add_rule({'source': clickhouse_node.ip_address, 'destination_port': 3306, 'action': 'DROP'})
        clickhouse_node.query("SELECT * FROM system.parts")
        clickhouse_node.query("SELECT * FROM system.mutations")
        clickhouse_node.query("SELECT * FROM system.graphite_retentions")

        clickhouse_node.query("DROP DATABASE test_db_disabled")
