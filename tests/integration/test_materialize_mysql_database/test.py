import os
import os.path as p
import subprocess
import time
import pwd
import re
import pymysql.cursors
import pytest
from helpers.cluster import ClickHouseCluster, get_docker_compose_path
import docker

from . import materialize_with_ddl

DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)
clickhouse_node = cluster.add_instance('node1', user_configs=["configs/users.xml"], with_mysql=False, stay_alive=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


class MySQLNodeInstance:
    def __init__(self, user='root', password='clickhouse', ip_address='127.0.0.1', port=3308, docker_compose=None, project_name=cluster.project_name):
        self.user = user
        self.port = port
        self.ip_address = ip_address
        self.password = password
        self.mysql_connection = None  # lazy init
        self.docker_compose = docker_compose
        self.project_name = project_name


    def alloc_connection(self):
        if self.mysql_connection is None:
            self.mysql_connection = pymysql.connect(user=self.user, password=self.password, host=self.ip_address,
                                                    port=self.port, autocommit=True)
        else:
            if self.mysql_connection.ping():
                self.mysql_connection = pymysql.connect(user=self.user, password=self.password, host=self.ip_address,
                                                        port=self.port, autocommit=True)
        return self.mysql_connection

    def query(self, execution_query):
        with self.alloc_connection().cursor() as cursor:
            cursor.execute(execution_query)

    def create_min_priv_user(self, user, password):
        self.query("CREATE USER '" + user + "'@'%' IDENTIFIED BY '" + password + "'")
        self.grant_min_priv_for_user(user)

    def grant_min_priv_for_user(self, user, db='priv_err_db'):
        self.query("GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO '" + user + "'@'%'")
        self.query("GRANT SELECT ON " + db + ".* TO '" + user + "'@'%'")

    def result(self, execution_query):
        with self.alloc_connection().cursor() as cursor:
            result = cursor.execute(execution_query)
            if result is not None:
                print(cursor.fetchall())

    def query_and_get_data(self, executio_query):
        with self.alloc_connection().cursor() as cursor:
            cursor.execute(executio_query)
            return cursor.fetchall()

    def close(self):
        if self.mysql_connection is not None:
            self.mysql_connection.close()

    def wait_mysql_to_start(self, timeout=60):
        start = time.time()
        while time.time() - start < timeout:
            try:
                self.alloc_connection()
                print("Mysql Started")
                return
            except Exception as ex:
                print("Can't connect to MySQL " + str(ex))
                time.sleep(0.5)

        subprocess.check_call(['docker-compose', 'ps', '--services', 'all'])
        raise Exception("Cannot wait MySQL container")

@pytest.fixture(scope="module")
def started_mysql_5_7():
    docker_compose = os.path.join(DOCKER_COMPOSE_PATH, 'docker_compose_mysql_5_7_for_materialize_mysql.yml')
    mysql_node = MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', 3308, docker_compose)

    try:
        subprocess.check_call(
            ['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'up', '--no-recreate', '-d'])
        mysql_node.wait_mysql_to_start(120)
        yield mysql_node
    finally:
        mysql_node.close()
        subprocess.check_call(['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'down', '--volumes',
                               '--remove-orphans'])


@pytest.fixture(scope="module")
def started_mysql_8_0():
    docker_compose = os.path.join(DOCKER_COMPOSE_PATH, 'docker_compose_mysql_8_0_for_materialize_mysql.yml')
    mysql_node = MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', 33308, docker_compose)

    try:
        subprocess.check_call(
            ['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'up', '--no-recreate', '-d'])
        mysql_node.wait_mysql_to_start(120)
        yield mysql_node
    finally:
        mysql_node.close()
        subprocess.check_call(['docker-compose', '-p', cluster.project_name, '-f', docker_compose, 'down', '--volumes',
                               '--remove-orphans'])


def test_materialize_database_dml_with_mysql_5_7(started_cluster, started_mysql_5_7):
    materialize_with_ddl.dml_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.materialize_mysql_database_with_datetime_and_decimal(clickhouse_node, started_mysql_5_7, "mysql1")


def test_materialize_database_dml_with_mysql_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.dml_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.materialize_mysql_database_with_datetime_and_decimal(clickhouse_node, started_mysql_8_0, "mysql8_0")



def test_materialize_database_ddl_with_mysql_5_7(started_cluster, started_mysql_5_7):
    try:
        materialize_with_ddl.drop_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
        materialize_with_ddl.create_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
        materialize_with_ddl.rename_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
        materialize_with_ddl.alter_add_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7,
                                                                              "mysql1")
        materialize_with_ddl.alter_drop_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7,
                                                                               "mysql1")
        # mysql 5.7 cannot support alter rename column
        # materialize_with_ddl.alter_rename_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
        materialize_with_ddl.alter_rename_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7,
                                                                                "mysql1")
        materialize_with_ddl.alter_modify_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7,
                                                                                 "mysql1")
    except:
        print((clickhouse_node.query(
            "select '\n', thread_id, query_id, arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym from system.stack_trace format TSVRaw")))
        raise


def test_materialize_database_ddl_with_mysql_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.drop_table_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.create_table_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.rename_table_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.alter_add_column_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0,
                                                                          "mysql8_0")
    materialize_with_ddl.alter_drop_column_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0,
                                                                           "mysql8_0")
    materialize_with_ddl.alter_rename_table_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0,
                                                                            "mysql8_0")
    materialize_with_ddl.alter_rename_column_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0,
                                                                             "mysql8_0")
    materialize_with_ddl.alter_modify_column_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0,
                                                                             "mysql8_0")


def test_materialize_database_ddl_with_empty_transaction_5_7(started_cluster, started_mysql_5_7):
    materialize_with_ddl.query_event_with_empty_transaction(clickhouse_node, started_mysql_5_7, "mysql1")


def test_materialize_database_ddl_with_empty_transaction_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.query_event_with_empty_transaction(clickhouse_node, started_mysql_8_0, "mysql8_0")


def test_select_without_columns_5_7(started_cluster, started_mysql_5_7):
    materialize_with_ddl.select_without_columns(clickhouse_node, started_mysql_5_7, "mysql1")


def test_select_without_columns_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.select_without_columns(clickhouse_node, started_mysql_8_0, "mysql8_0")


def test_insert_with_modify_binlog_checksum_5_7(started_cluster, started_mysql_5_7):
    materialize_with_ddl.insert_with_modify_binlog_checksum(clickhouse_node, started_mysql_5_7, "mysql1")


def test_insert_with_modify_binlog_checksum_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.insert_with_modify_binlog_checksum(clickhouse_node, started_mysql_8_0, "mysql8_0")


def test_materialize_database_err_sync_user_privs_5_7(started_cluster, started_mysql_5_7):
    materialize_with_ddl.err_sync_user_privs_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")

def test_materialize_database_err_sync_user_privs_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.err_sync_user_privs_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")


def test_network_partition_5_7(started_cluster, started_mysql_5_7):
    materialize_with_ddl.network_partition_test(clickhouse_node, started_mysql_5_7, "mysql1")

def test_network_partition_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.network_partition_test(clickhouse_node, started_mysql_8_0, "mysql8_0")


def test_mysql_kill_sync_thread_restore_5_7(started_cluster, started_mysql_5_7):
    materialize_with_ddl.mysql_kill_sync_thread_restore_test(clickhouse_node, started_mysql_5_7, "mysql1")

def test_mysql_kill_sync_thread_restore_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.mysql_kill_sync_thread_restore_test(clickhouse_node, started_mysql_8_0, "mysql8_0")


def test_mysql_killed_while_insert_5_7(started_cluster, started_mysql_5_7):
    materialize_with_ddl.mysql_killed_while_insert(clickhouse_node, started_mysql_5_7, "mysql1")

def test_mysql_killed_while_insert_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.mysql_killed_while_insert(clickhouse_node, started_mysql_8_0, "mysql8_0")


def test_clickhouse_killed_while_insert_5_7(started_cluster, started_mysql_5_7):
    materialize_with_ddl.clickhouse_killed_while_insert(clickhouse_node, started_mysql_5_7, "mysql1")

def test_clickhouse_killed_while_insert_8_0(started_cluster, started_mysql_8_0):
    materialize_with_ddl.clickhouse_killed_while_insert(clickhouse_node, started_mysql_8_0, "mysql8_0")
