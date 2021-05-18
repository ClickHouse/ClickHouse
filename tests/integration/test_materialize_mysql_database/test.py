import os
import os.path as p
import time
import pwd
import re
import pymysql.cursors
import pytest
from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check
import docker

from . import materialize_with_ddl

DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)

node_db_ordinary = cluster.add_instance('node1', user_configs=["configs/users.xml"], with_mysql=False, stay_alive=True)
node_db_atomic = cluster.add_instance('node2', user_configs=["configs/users_db_atomic.xml"], with_mysql=False, stay_alive=True)
node_disable_bytes_settings = cluster.add_instance('node3', user_configs=["configs/users_disable_bytes_settings.xml"], with_mysql=False, stay_alive=True)
node_disable_rows_settings = cluster.add_instance('node4', user_configs=["configs/users_disable_rows_settings.xml"], with_mysql=False, stay_alive=True)

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

        self.base_dir = p.dirname(__file__)
        self.instances_dir = p.join(self.base_dir, '_instances_mysql')
        if not os.path.exists(self.instances_dir):
            os.mkdir(self.instances_dir)
        self.docker_logs_path = p.join(self.instances_dir, 'docker_mysql.log')
        self.start_up = False

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

    def start_and_wait(self):
        if self.start_up:
            return

        run_and_check(['docker-compose',
                       '-p', cluster.project_name,
                       '-f', self.docker_compose,
                       'up', '--no-recreate', '-d',
                       ])
        self.wait_mysql_to_start(120)
        self.start_up = True

    def close(self):
        if self.mysql_connection is not None:
            self.mysql_connection.close()

        with open(self.docker_logs_path, "w+") as f:
            try:
                run_and_check([
                    'docker-compose',
                    '-p', cluster.project_name,
                    '-f', self.docker_compose, 'logs',
                ], stdout=f)
            except Exception as e:
                print("Unable to get logs from docker mysql.")

        self.start_up = False

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

        run_and_check(['docker-compose', 'ps', '--services', 'all'])
        raise Exception("Cannot wait MySQL container")


mysql_5_7_docker_compose = os.path.join(DOCKER_COMPOSE_PATH, 'docker_compose_mysql_5_7_for_materialize_mysql.yml')
mysql_5_7_node = MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', 3308, mysql_5_7_docker_compose)

mysql_8_0_docker_compose = os.path.join(DOCKER_COMPOSE_PATH, 'docker_compose_mysql_8_0_for_materialize_mysql.yml')
mysql_8_0_node = MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', 3309, mysql_8_0_docker_compose)


@pytest.fixture(scope="module")
def started_mysql_5_7():
    try:
        mysql_5_7_node.start_and_wait()
        yield mysql_5_7_node
    finally:
        mysql_5_7_node.close()
        run_and_check(['docker-compose', '-p', cluster.project_name, '-f', mysql_5_7_docker_compose, 'down', '--volumes', '--remove-orphans'])


@pytest.fixture(scope="module")
def started_mysql_8_0():
    try:
        mysql_8_0_node.start_and_wait()
        yield mysql_8_0_node
    finally:
        mysql_8_0_node.close()
        run_and_check(['docker-compose', '-p', cluster.project_name, '-f', mysql_8_0_docker_compose, 'down', '--volumes', '--remove-orphans'])


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_materialize_database_dml_with_mysql_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.dml_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.materialize_mysql_database_with_views(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.materialize_mysql_database_with_datetime_and_decimal(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.move_to_prewhere_and_column_filtering(clickhouse_node, started_mysql_5_7, "mysql1")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_materialize_database_dml_with_mysql_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.dml_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.materialize_mysql_database_with_views(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.materialize_mysql_database_with_datetime_and_decimal(clickhouse_node, started_mysql_8_0, "mysql8_0")
    materialize_with_ddl.move_to_prewhere_and_column_filtering(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_materialize_database_ddl_with_mysql_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.drop_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.create_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.rename_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.alter_add_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.alter_drop_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
    # mysql 5.7 cannot support alter rename column
    # materialize_with_ddl.alter_rename_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.alter_rename_table_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.alter_modify_column_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_materialize_database_ddl_with_mysql_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
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


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_materialize_database_ddl_with_empty_transaction_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.query_event_with_empty_transaction(clickhouse_node, started_mysql_5_7, "mysql1")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_materialize_database_ddl_with_empty_transaction_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.query_event_with_empty_transaction(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_select_without_columns_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.select_without_columns(clickhouse_node, started_mysql_5_7, "mysql1")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_select_without_columns_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.select_without_columns(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_insert_with_modify_binlog_checksum_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.insert_with_modify_binlog_checksum(clickhouse_node, started_mysql_5_7, "mysql1")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_insert_with_modify_binlog_checksum_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.insert_with_modify_binlog_checksum(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_materialize_database_err_sync_user_privs_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.err_sync_user_privs_with_materialize_mysql_database(clickhouse_node, started_mysql_5_7, "mysql1")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_materialize_database_err_sync_user_privs_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.err_sync_user_privs_with_materialize_mysql_database(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_network_partition_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.network_partition_test(clickhouse_node, started_mysql_5_7, "mysql1")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_network_partition_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.network_partition_test(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_mysql_kill_sync_thread_restore_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.mysql_kill_sync_thread_restore_test(clickhouse_node, started_mysql_5_7, "mysql1")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_mysql_kill_sync_thread_restore_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.mysql_kill_sync_thread_restore_test(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_mysql_killed_while_insert_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.mysql_killed_while_insert(clickhouse_node, started_mysql_5_7, "mysql1")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_mysql_killed_while_insert_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.mysql_killed_while_insert(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_clickhouse_killed_while_insert_5_7(started_cluster, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.clickhouse_killed_while_insert(clickhouse_node, started_mysql_5_7, "mysql1")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_atomic])
def test_clickhouse_killed_while_insert_8_0(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.clickhouse_killed_while_insert(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_ordinary])
def test_utf8mb4(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.utf8mb4_test(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.utf8mb4_test(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_ordinary])
def test_system_parts_table(started_cluster, started_mysql_8_0, clickhouse_node):
    materialize_with_ddl.system_parts_test(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_ordinary])
def test_multi_table_update(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.multi_table_update_test(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.multi_table_update_test(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_db_ordinary, node_db_ordinary])
def test_system_tables_table(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.system_tables_test(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.system_tables_test(clickhouse_node, started_mysql_8_0, "mysql8_0")


@pytest.mark.parametrize(('clickhouse_node'), [node_disable_bytes_settings, node_disable_rows_settings])
def test_mysql_settings(started_cluster, started_mysql_8_0, started_mysql_5_7, clickhouse_node):
    materialize_with_ddl.mysql_settings_test(clickhouse_node, started_mysql_5_7, "mysql1")
    materialize_with_ddl.mysql_settings_test(clickhouse_node, started_mysql_8_0, "mysql8_0")
