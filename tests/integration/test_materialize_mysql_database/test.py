import os
import subprocess
import time

import pymysql.cursors
import pytest

import materialize_with_ddl
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__)
clickhouse_node = cluster.add_instance('node1', config_dir="configs", with_mysql=False)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


class MySQLNodeInstance:
    def __init__(self, user='root', password='clickhouse', hostname='127.0.0.1', port=3308):
        self.user = user
        self.port = port
        self.hostname = hostname
        self.password = password
        self.mysql_connection = None  # lazy init

    def alloc_connection(self):
        if self.mysql_connection is None:
            self.mysql_connection = pymysql.connect(user=self.user, password=self.password, host=self.hostname, port=self.port, autocommit=True)
        return self.mysql_connection

    def query(self, execution_query):
        with self.alloc_connection().cursor() as cursor:
            cursor.execute(execution_query)

    def close(self):
        if self.mysql_connection is not None:
            self.mysql_connection.close()

    def wait_mysql_to_start(self, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                self.alloc_connection()
                print "Mysql Started"
                return
            except Exception as ex:
                print "Can't connect to MySQL " + str(ex)
                time.sleep(0.5)

        subprocess.check_call(['docker-compose', 'ps', '--services', 'all'])
        raise Exception("Cannot wait MySQL container")


@pytest.fixture(scope="module")
def started_mysql_5_7():
    mysql_node = MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', 33307)
    docker_compose = os.path.join(SCRIPT_DIR, 'composes', 'mysql_5_7_compose.yml')

    try:
        subprocess.check_call(['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'up', '--no-recreate', '-d'])
        mysql_node.wait_mysql_to_start(120)
        yield mysql_node
    finally:
        mysql_node.close()
        subprocess.check_call(['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'down', '--volumes', '--remove-orphans'])


@pytest.fixture(scope="module")
def started_mysql_8_0():
    mysql_node = MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', 33308)
    docker_compose = os.path.join(SCRIPT_DIR, 'composes', 'mysql_8_0_compose.yml')

    try:
        subprocess.check_call(['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'up', '--no-recreate', '-d'])
        mysql_node.wait_mysql_to_start(120)
        yield mysql_node
    finally:
        mysql_node.close()
        subprocess.check_call(['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'down', '--volumes', '--remove-orphans'])


def test_materialize_database_dml_with_mysql_5_7(started_cluster, started_mysql_5_7):
    materialize_with_ddl.dml_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql5_7")


def test_materialize_database_dml_with_mysql_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.dml_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")

def test_materialize_database_ddl_with_mysql_5_7(started_cluster, started_mysql_5_7):
    try:
        materialize_with_ddl.drop_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql5_7")
        materialize_with_ddl.create_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql5_7")
        materialize_with_ddl.rename_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql5_7")
        materialize_with_ddl.alter_add_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql5_7")
        materialize_with_ddl.alter_drop_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql5_7")
        # mysql 5.7 cannot support alter rename column
        # materialize_with_ddl.alter_rename_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql5_7")
        materialize_with_ddl.alter_rename_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql5_7")
        materialize_with_ddl.alter_modify_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql5_7")
    except:
        print(clickhouse_node.query("select '\n', thread_id, query_id, arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym from system.stack_trace format TSVRaw"))
        raise


def test_materialize_database_ddl_with_mysql_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.drop_table_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.create_table_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.rename_table_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.alter_add_column_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.alter_drop_column_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.alter_rename_table_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.alter_rename_column_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.alter_modify_column_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")

