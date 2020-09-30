import pytest
import pymysql.cursors
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', with_mysql=True)


def get_mysql_conn():
    conn = pymysql.connect(user='root', password='clickhouse', host='127.0.0.1', port=3308)
    return conn


def create_mysql_db(conn, name):
    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))


def create_mysql_table(conn, table_name):
    create_table_sql_template = """
        CREATE TABLE `clickhouse`.`{}` (
        `id` int(11) NOT NULL,
        `name` varchar(50) NOT NULL,
        `age` int  NOT NULL default 0,
        `money` int NOT NULL default 0,
        PRIMARY KEY (`id`)) ENGINE=InnoDB;
    """

    with conn.cursor() as cursor:
        cursor.execute(create_table_sql_template.format(table_name))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        conn = get_mysql_conn()
        create_mysql_db(conn, 'clickhouse')
        yield cluster

    finally:
        cluster.shutdown()


def test_simple_select(started_cluster):
    table_name = 'test_simple_select'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    node1.query("""
    CREATE TABLE {table_name} (
        id UInt32,
        name String,
        age UInt32,
        money UInt32,
        sign Int8,
        version UInt64
    )
    ENGINE = MySQLReplica('mysql1:3306', 'clickhouse', '{table_name}', 'root', 'clickhouse')
    SETTINGS
        max_rows_in_buffer=1,
        max_bytes_in_buffer=1,
        max_rows_in_buffers=1,
        max_bytes_in_buffers=1,
        max_flush_data_time=1""".format(table_name=table_name))

    assert node1.query("SELECT * FROM {table_name}".format(table_name=table_name)) == ""

    with conn.cursor() as cursor:
        cursor.execute("INSERT INTO clickhouse.{table_name} VALUES (1, 'Hello', 22, 100)".format(table_name=table_name))

    time.sleep(3)

    assert node1.query("SELECT * FROM {table_name}".format(table_name=table_name)) == "1\tHello\t22\t100\t1\t2\n"

    conn.close()
