import os
import os.path as p
import time
import pwd
import re
import pymysql.cursors
import pytest
from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check
import docker
import logging

from . import materialize_with_ddl

DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)
mysql_node = None
mysql8_node = None

node_db_ordinary = cluster.add_instance('node1', user_configs=["configs/users.xml"], with_mysql=True, stay_alive=True)
node_db_atomic = cluster.add_instance('node2', user_configs=["configs/users_db_atomic.xml"], with_mysql8=True, stay_alive=True)
node_disable_bytes_settings = cluster.add_instance('node3', user_configs=["configs/users_disable_bytes_settings.xml"], with_mysql=False, stay_alive=True)
node_disable_rows_settings = cluster.add_instance('node4', user_configs=["configs/users_disable_rows_settings.xml"], with_mysql=False, stay_alive=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


class MySQLConnection:
    def __init__(self, port, user='root', password='clickhouse', ip_address=None, docker_compose=None, project_name=cluster.project_name):
        self.user = user
        self.port = port
        self.ip_address = ip_address
        self.password = password
        self.mysql_connection = None  # lazy init

    def alloc_connection(self):
        errors = []
        for _ in range(5):
            try:
                if self.mysql_connection is None:
                    self.mysql_connection = pymysql.connect(user=self.user, password=self.password, host=self.ip_address,
                                                            port=self.port, autocommit=True)
                else:
                    self.mysql_connection.ping(reconnect=True)
                logging.debug("MySQL Connection establised: {}:{}".format(self.ip_address, self.port))
                return self.mysql_connection
            except Exception as e:
                errors += [str(e)]
                time.sleep(1)
        raise Exception("Connection not establised, {}".format(errors))

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

@pytest.fixture(scope="module")
def started_mysql_5_7():
    mysql_node = MySQLConnection(cluster.mysql_port, 'root', 'clickhouse', cluster.mysql_ip)
    yield mysql_node

@pytest.fixture(scope="module")
def started_mysql_8_0():
    mysql8_node = MySQLConnection(cluster.mysql8_port, 'root', 'clickhouse', cluster.mysql8_ip)
    yield mysql8_node

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_materialize_database_dml_with_mysql_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.dml_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.materialized_mysql_database_with_views(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.materialized_mysql_database_with_datetime_and_decimal(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.move_to_prewhere_and_column_filtering(clickhouse_node, started_mysql_5_7, "mysql57")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_materialize_database_dml_with_mysql_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.dml_with_materialized_mysql_database(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.materialized_mysql_database_with_views(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.materialized_mysql_database_with_datetime_and_decimal(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.move_to_prewhere_and_column_filtering(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_materialize_database_ddl_with_mysql_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.drop_table_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.create_table_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.rename_table_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.alter_add_column_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.alter_drop_column_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")
    # mysql 5.7 cannot support alter rename column
    # materialize_with_ddl.alter_rename_column_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.alter_rename_table_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.alter_modify_column_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_materialize_database_ddl_with_mysql_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.drop_table_with_materialized_mysql_database(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.create_table_with_materialized_mysql_database(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.rename_table_with_materialized_mysql_database(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.alter_add_column_with_materialized_mysql_database(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.alter_drop_column_with_materialized_mysql_database(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.alter_rename_table_with_materialized_mysql_database(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.alter_rename_column_with_materialized_mysql_database(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.alter_modify_column_with_materialized_mysql_database(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_materialize_database_ddl_with_empty_transaction_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.query_event_with_empty_transaction(clickhouse_node, started_mysql_5_7, "mysql57")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_materialize_database_ddl_with_empty_transaction_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.query_event_with_empty_transaction(clickhouse_node, started_mysql_8_0, "mysql80")


@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_select_without_columns_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.select_without_columns(clickhouse_node, started_mysql_5_7, "mysql57")


@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_select_without_columns_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.select_without_columns(clickhouse_node, started_mysql_8_0, "mysql80")


@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_insert_with_modify_binlog_checksum_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.insert_with_modify_binlog_checksum(clickhouse_node, started_mysql_5_7, "mysql57")


@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_insert_with_modify_binlog_checksum_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.insert_with_modify_binlog_checksum(clickhouse_node, started_mysql_8_0, "mysql80")


@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_materialize_database_err_sync_user_privs_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.err_sync_user_privs_with_materialized_mysql_database(clickhouse_node, started_mysql_5_7, "mysql57")


@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_materialize_database_err_sync_user_privs_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.err_sync_user_privs_with_materialized_mysql_database(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_network_partition_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.network_partition_test(clickhouse_node, started_mysql_5_7, "mysql57")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_network_partition_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.network_partition_test(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_mysql_kill_sync_thread_restore_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.mysql_kill_sync_thread_restore_test(clickhouse_node, started_mysql_5_7, "mysql57")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_mysql_kill_sync_thread_restore_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.mysql_kill_sync_thread_restore_test(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_mysql_killed_while_insert_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.mysql_killed_while_insert(clickhouse_node, started_mysql_5_7, "mysql57")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_mysql_killed_while_insert_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.mysql_killed_while_insert(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_clickhouse_killed_while_insert_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.clickhouse_killed_while_insert(clickhouse_node, started_mysql_5_7, "mysql57")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_clickhouse_killed_while_insert_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.clickhouse_killed_while_insert(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_ordinary])
def test_utf8mb4(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.utf8mb4_test(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.utf8mb4_test(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_ordinary])
def test_system_parts_table(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.system_parts_test(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_ordinary])
def test_multi_table_update(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.multi_table_update_test(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.multi_table_update_test(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_ordinary])
def test_system_tables_table(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.system_tables_test(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.system_tables_test(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_ordinary])
def test_materialize_with_column_comments(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.materialize_with_column_comments_test(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.materialize_with_column_comments_test(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_ordinary])
def test_materialize_with_enum(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.materialize_with_enum8_test(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.materialize_with_enum16_test(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.alter_enum8_to_enum16_test(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.materialize_with_enum8_test(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.materialize_with_enum16_test(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.alter_enum8_to_enum16_test(clickhouse_node, started_mysql_8_0, "mysql80")


@pytest.mark.parametrize(('clickhouse_node'), [node_disable_bytes_settings, node_disable_rows_settings])
def test_mysql_settings(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.mysql_settings_test(clickhouse_node, started_mysql_5_7, "mysql57")
    materialize_with_ddl.mysql_settings_test(clickhouse_node, started_mysql_8_0, "mysql80")

@pytest.mark.parametrize(('clickhouse_node'), [pytest.param(node_db_ordinary, id="ordinary"), pytest.param(node_db_atomic, id="atomic")])
def test_large_transaction(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.materialized_mysql_large_transaction(clickhouse_node, started_mysql_8_0, "mysql80")
    materialize_with_ddl.materialized_mysql_large_transaction(clickhouse_node, started_mysql_5_7, "mysql57")
