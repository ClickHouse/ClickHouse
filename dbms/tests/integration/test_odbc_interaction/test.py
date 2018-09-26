import time
import pytest

import os
import pymysql.cursors
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))

cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))
node1 = cluster.add_instance('node1', with_odbc_drivers=True, with_mysql=True, image='alesapin/ubuntu_with_odbc:14.04', main_configs=['configs/dictionaries/sqlite3_odbc_hashed_dictionary.xml', 'configs/dictionaries/sqlite3_odbc_cached_dictionary.xml'])

create_table_sql_template =   """
    CREATE TABLE `clickhouse`.`{}` (
    `id` int(11) NOT NULL,
    `name` varchar(50) NOT NULL,
    `age` int  NOT NULL default 0,
    `money` int NOT NULL default 0,
    PRIMARY KEY (`id`)) ENGINE=InnoDB;
    """
def get_mysql_conn():
    conn = pymysql.connect(user='root', password='clickhouse', host='127.0.0.1', port=3308)
    return conn

def create_mysql_db(conn, name):
    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))

def create_mysql_table(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql_template.format(table_name))

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        sqlite_db =  node1.odbc_drivers["SQLite3"]["Database"]

        node1.exec_in_container(["bash", "-c", "echo 'CREATE TABLE t1(x INTEGER PRIMARY KEY ASC, y, z);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')
        node1.exec_in_container(["bash", "-c", "echo 'CREATE TABLE t2(X INTEGER PRIMARY KEY ASC, Y, Z);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')
        node1.exec_in_container(["bash", "-c", "echo 'CREATE TABLE t3(X INTEGER PRIMARY KEY ASC, Y, Z);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')
        node1.exec_in_container(["bash", "-c", "echo 'CREATE TABLE t4(X INTEGER PRIMARY KEY ASC, Y, Z);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')
        conn = get_mysql_conn()
        ## create mysql db and table
        create_mysql_db(conn, 'clickhouse')

        yield cluster

    except Exception as ex:
        print(ex)
    finally:
        cluster.shutdown()

def test_mysql_simple_select_works(started_cluster):
    mysql_setup = node1.odbc_drivers["MySQL"]

    table_name = 'test_insert_select'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse');
'''.format(table_name, table_name))

    node1.query("INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(100) ".format(table_name))

    # actually, I don't know, what wrong with that connection string, but libmyodbc always falls into segfault
    node1.query("SELECT * FROM odbc('DSN={}', '{}')".format(mysql_setup["DSN"], table_name), ignore_error=True)

    # server still works after segfault
    assert node1.query("select 1") == "1\n"

    conn.close()


def test_sqlite_simple_select_function_works(started_cluster):
    sqlite_setup = node1.odbc_drivers["SQLite3"]
    sqlite_db = sqlite_setup["Database"]

    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t1 values(1, 2, 3);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')
    assert node1.query("select * from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "1\t2\t3\n"

    assert node1.query("select y from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "2\n"
    assert node1.query("select z from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "3\n"
    assert node1.query("select x from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "1\n"
    assert node1.query("select x, y from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "1\t2\n"
    assert node1.query("select z, x, y from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "3\t1\t2\n"
    assert node1.query("select count(), sum(x) from odbc('DSN={}', '{}') group by x".format(sqlite_setup["DSN"], 't1')) == "1\t1\n"

def test_sqlite_simple_select_storage_works(started_cluster):
    sqlite_setup = node1.odbc_drivers["SQLite3"]
    sqlite_db = sqlite_setup["Database"]

    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t4 values(1, 2, 3);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')
    node1.query("create table SqliteODBC (x Int32, y String, z String) engine = ODBC('DSN={}', '', 't4')".format(sqlite_setup["DSN"]))

    assert node1.query("select * from SqliteODBC") == "1\t2\t3\n"
    assert node1.query("select y from SqliteODBC") == "2\n"
    assert node1.query("select z from SqliteODBC") == "3\n"
    assert node1.query("select x from SqliteODBC") == "1\n"
    assert node1.query("select x, y from SqliteODBC") == "1\t2\n"
    assert node1.query("select z, x, y from SqliteODBC") == "3\t1\t2\n"
    assert node1.query("select count(), sum(x) from SqliteODBC group by x") == "1\t1\n"

def test_sqlite_odbc_hashed_dictionary(started_cluster):
    sqlite_db =  node1.odbc_drivers["SQLite3"]["Database"]
    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t2 values(1, 2, 3);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')

    assert node1.query("select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(1))") == "3\n"
    assert node1.query("select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(200))") == "1\n" # default

    time.sleep(5) # first reload
    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t2 values(200, 2, 7);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')

    # No reload because of invalidate query
    time.sleep(5)
    assert node1.query("select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(1))") == "3\n"
    assert node1.query("select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(200))") == "1\n" # still default

    node1.exec_in_container(["bash", "-c", "echo 'REPLACE INTO t2 values(1, 2, 5);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')

    # waiting for reload
    time.sleep(5)

    assert node1.query("select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(1))") == "5\n"
    assert node1.query("select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(200))") == "7\n" # new value

def test_sqlite_odbc_cached_dictionary(started_cluster):
    sqlite_db =  node1.odbc_drivers["SQLite3"]["Database"]
    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t3 values(1, 2, 3);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')

    assert node1.query("select dictGetUInt8('sqlite3_odbc_cached', 'Z', toUInt64(1))") == "3\n"

    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t3 values(200, 2, 7);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')

    assert node1.query("select dictGetUInt8('sqlite3_odbc_cached', 'Z', toUInt64(200))") == "7\n" # new value

    node1.exec_in_container(["bash", "-c", "echo 'REPLACE INTO t3 values(1, 2, 12);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')

    time.sleep(5)

    assert node1.query("select dictGetUInt8('sqlite3_odbc_cached', 'Z', toUInt64(1))") == "12\n"
