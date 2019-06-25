from contextlib import contextmanager

import time
import pytest

## sudo -H pip install PyMySQL
import pymysql.cursors

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_mysql = True)
create_table_normal_sql_template = """
    CREATE TABLE `clickhouse`.`{}` (
        `id` int(11) NOT NULL,
        `name` varchar(50) NOT NULL,
        `age` int  NOT NULL default 0,
        `money` int NOT NULL default 0,
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB;
    """

create_table_mysql_style_sql_template = """
    CREATE TABLE `clickhouse`.`{}` (
        `id` int(11) NOT NULL,
        `float` float NOT NULL,
        `Float32` float NOT NULL,
        `test``name` varchar(50) NOT NULL,
        PRIMARY KEY (`id`)
    ) ENGINE=InnoDB;
    """

drop_table_sql_template = "DROP TABLE `clickhouse`.`{}`"

add_column_sql_template = "ALTER TABLE `clickhouse`.`{}` ADD COLUMN `pid` int(11)"
del_column_sql_template = "ALTER TABLE `clickhouse`.`{}` DROP COLUMN `pid`"


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        conn = get_mysql_conn()
        ## create mysql db and table
        create_mysql_db(conn, 'clickhouse')
        node1.query("CREATE DATABASE clickhouse_mysql ENGINE = MySQL('mysql1:3306', 'clickhouse', 'root', 'clickhouse')")
        yield cluster

    finally:
        cluster.shutdown()


def test_sync_tables_list_between_clickhouse_and_mysql(started_cluster):
    mysql_connection = get_mysql_conn()
    assert node1.query('SHOW TABLES FROM clickhouse_mysql FORMAT TSV').rstrip() == ''

    create_normal_mysql_table(mysql_connection, 'first_mysql_table')
    assert node1.query("SHOW TABLES FROM clickhouse_mysql LIKE 'first_mysql_table' FORMAT TSV").rstrip() == 'first_mysql_table'

    create_normal_mysql_table(mysql_connection, 'second_mysql_table')
    assert node1.query("SHOW TABLES FROM clickhouse_mysql LIKE 'second_mysql_table' FORMAT TSV").rstrip() == 'second_mysql_table'

    drop_mysql_table(mysql_connection, 'second_mysql_table')
    assert node1.query("SHOW TABLES FROM clickhouse_mysql LIKE 'second_mysql_table' FORMAT TSV").rstrip() == ''

    mysql_connection.close()

def test_sync_tables_structure_between_clickhouse_and_mysql(started_cluster):
    mysql_connection = get_mysql_conn()

    create_normal_mysql_table(mysql_connection, 'test_sync_column')

    assert node1.query(
        "SELECT name FROM system.columns WHERE table = 'test_sync_column' AND database = 'clickhouse_mysql' AND name = 'pid' ").rstrip() == ''

    time.sleep(3)
    add_mysql_table_column(mysql_connection, "test_sync_column")

    assert node1.query(
        "SELECT name FROM system.columns WHERE table = 'test_sync_column' AND database = 'clickhouse_mysql' AND name = 'pid' ").rstrip() == 'pid'

    time.sleep(3)
    drop_mysql_table_column(mysql_connection, "test_sync_column")
    assert node1.query(
        "SELECT name FROM system.columns WHERE table = 'test_sync_column' AND database = 'clickhouse_mysql' AND name = 'pid' ").rstrip() == ''

    mysql_connection.close()

def test_insert_select(started_cluster):
    mysql_connection = get_mysql_conn()
    create_normal_mysql_table(mysql_connection, 'test_insert_select')

    assert node1.query("SELECT count() FROM `clickhouse_mysql`.{}".format('test_insert_select')).rstrip() == '0'
    node1.query("INSERT INTO `clickhouse_mysql`.{}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format('test_insert_select'))
    assert node1.query("SELECT count() FROM `clickhouse_mysql`.{}".format('test_insert_select')).rstrip() == '10000'
    assert node1.query("SELECT sum(money) FROM `clickhouse_mysql`.{}".format('test_insert_select')).rstrip() == '30000'
    mysql_connection.close()

def test_insert_select_with_mysql_style_table(started_cluster):
    mysql_connection = get_mysql_conn()
    create_mysql_style_mysql_table(mysql_connection, 'test_mysql``_style_table')

    assert node1.query("SELECT count() FROM `clickhouse_mysql`.`{}`".format('test_mysql\`_style_table')).rstrip() == '0'
    node1.query("INSERT INTO `clickhouse_mysql`.`{}`(id, `float`, `Float32`, `test\`name`) select number, 3, 3, 'name' from numbers(10000) ".format('test_mysql\`_style_table'))
    assert node1.query("SELECT count() FROM `clickhouse_mysql`.`{}`".format('test_mysql\`_style_table')).rstrip() == '10000'
    assert node1.query("SELECT sum(`float`) FROM `clickhouse_mysql`.`{}`".format('test_mysql\`_style_table')).rstrip() == '30000'
    mysql_connection.close()

def get_mysql_conn():
    conn = pymysql.connect(user='root', password='clickhouse', host='127.0.0.1', port=3308)
    return conn

def create_mysql_db(conn, name):
    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))

def create_normal_mysql_table(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(create_table_normal_sql_template.format(table_name))

def create_mysql_style_mysql_table(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(create_table_mysql_style_sql_template.format(table_name))

def drop_mysql_table(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(drop_table_sql_template.format(table_name))

def add_mysql_table_column(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(add_column_sql_template.format(table_name))

def drop_mysql_table_column(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(del_column_sql_template.format(table_name))
