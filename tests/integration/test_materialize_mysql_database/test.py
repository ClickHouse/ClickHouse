import os
import subprocess

import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path
from helpers.mysql_helpers import MySQLNodeInstance
from . import materialize_with_ddl

DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)
clickhouse_node = cluster.add_instance('node1', user_configs=["configs/users.xml"], with_mysql=False)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def started_mysql_5_7():
    mysql_node = MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', 3308)
    docker_compose = os.path.join(DOCKER_COMPOSE_PATH, 'docker_compose_mysql.yml')

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
    mysql_node = MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', 33308)
    docker_compose = os.path.join(DOCKER_COMPOSE_PATH, 'docker_compose_mysql_8_0.yml')

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
