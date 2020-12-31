import time

import psycopg2
import pymysql.cursors
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_odbc_drivers=True, with_mysql=True,
                             main_configs=['configs/openssl.xml', 'configs/odbc_logging.xml',
                                           'configs/enable_dictionaries.xml',
                                           'configs/dictionaries/sqlite3_odbc_hashed_dictionary.xml',
                                           'configs/dictionaries/sqlite3_odbc_cached_dictionary.xml',
                                           'configs/dictionaries/postgres_odbc_hashed_dictionary.xml'], stay_alive=True)

create_table_sql_template = """
    CREATE TABLE `clickhouse`.`{}` (
    `id` int(11) NOT NULL,
    `name` varchar(50) NOT NULL,
    `age` int  NOT NULL default 0,
    `money` int NOT NULL default 0,
    `column_x` int default NULL,
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


def get_postgres_conn():
    conn_string = "host='localhost' user='postgres' password='mysecretpassword'"
    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True
    return conn


def create_postgres_db(conn, name):
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA {}".format(name))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        sqlite_db = node1.odbc_drivers["SQLite3"]["Database"]

        print("sqlite data received")
        node1.exec_in_container(
            ["bash", "-c", "echo 'CREATE TABLE t1(x INTEGER PRIMARY KEY ASC, y, z);' | sqlite3 {}".format(sqlite_db)],
            privileged=True, user='root')
        node1.exec_in_container(
            ["bash", "-c", "echo 'CREATE TABLE t2(X INTEGER PRIMARY KEY ASC, Y, Z);' | sqlite3 {}".format(sqlite_db)],
            privileged=True, user='root')
        node1.exec_in_container(
            ["bash", "-c", "echo 'CREATE TABLE t3(X INTEGER PRIMARY KEY ASC, Y, Z);' | sqlite3 {}".format(sqlite_db)],
            privileged=True, user='root')
        node1.exec_in_container(
            ["bash", "-c", "echo 'CREATE TABLE t4(X INTEGER PRIMARY KEY ASC, Y, Z);' | sqlite3 {}".format(sqlite_db)],
            privileged=True, user='root')
        print("sqlite tables created")
        mysql_conn = get_mysql_conn()
        print("mysql connection received")
        ## create mysql db and table
        create_mysql_db(mysql_conn, 'clickhouse')
        print("mysql database created")

        postgres_conn = get_postgres_conn()
        print("postgres connection received")

        create_postgres_db(postgres_conn, 'clickhouse')
        print("postgres db created")

        cursor = postgres_conn.cursor()
        cursor.execute(
            "create table if not exists clickhouse.test_table (column1 int primary key, column2 varchar(40) not null)")

        yield cluster

    except Exception as ex:
        print(ex)
        raise ex
    finally:
        cluster.shutdown()


def test_mysql_simple_select_works(started_cluster):
    mysql_setup = node1.odbc_drivers["MySQL"]

    table_name = 'test_insert_select'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    # Check that NULL-values are handled correctly by the ODBC-bridge
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO clickhouse.{} VALUES(50, 'null-guy', 127, 255, NULL), (100, 'non-null-guy', 127, 255, 511);".format(
                table_name))
        conn.commit()
    assert node1.query("SELECT column_x FROM odbc('DSN={}', '{}')".format(mysql_setup["DSN"], table_name),
                       settings={"external_table_functions_use_nulls": "1"}) == '\\N\n511\n'
    assert node1.query("SELECT column_x FROM odbc('DSN={}', '{}')".format(mysql_setup["DSN"], table_name),
                       settings={"external_table_functions_use_nulls": "0"}) == '0\n511\n'

    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32, column_x Nullable(UInt32)) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse');
'''.format(table_name, table_name))

    node1.query(
        "INSERT INTO {}(id, name, money, column_x) select number, concat('name_', toString(number)), 3, NULL from numbers(49) ".format(
            table_name))
    node1.query(
        "INSERT INTO {}(id, name, money, column_x) select number, concat('name_', toString(number)), 3, 42 from numbers(51, 49) ".format(
            table_name))

    assert node1.query("SELECT COUNT () FROM {} WHERE column_x IS NOT NULL".format(table_name)) == '50\n'
    assert node1.query("SELECT COUNT () FROM {} WHERE column_x IS NULL".format(table_name)) == '50\n'
    assert node1.query("SELECT count(*) FROM odbc('DSN={}', '{}')".format(mysql_setup["DSN"], table_name)) == '100\n'

    # previously this test fails with segfault
    # just to be sure :)
    assert node1.query("select 1") == "1\n"

    conn.close()


def test_mysql_insert(started_cluster):
    mysql_setup = node1.odbc_drivers["MySQL"]
    table_name = 'test_insert'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)
    odbc_args = "'DSN={}', '{}', '{}'".format(mysql_setup["DSN"], mysql_setup["Database"], table_name)

    node1.query(
        "create table mysql_insert (id Int64, name String, age UInt8, money Float, column_x Nullable(Int16)) Engine=ODBC({})".format(
            odbc_args))
    node1.query("insert into mysql_insert values (1, 'test', 11, 111, 1111), (2, 'odbc', 22, 222, NULL)")
    assert node1.query("select * from mysql_insert") == "1\ttest\t11\t111\t1111\n2\todbc\t22\t222\t\\N\n"

    node1.query("insert into table function odbc({}) values (3, 'insert', 33, 333, 3333)".format(odbc_args))
    node1.query(
        "insert into table function odbc({}) (id, name, age, money) select id*4, upper(name), age*4, money*4 from odbc({}) where id=1".format(
            odbc_args, odbc_args))
    assert node1.query(
        "select * from mysql_insert where id in (3, 4)") == "3\tinsert\t33\t333\t3333\n4\tTEST\t44\t444\t\\N\n"


def test_sqlite_simple_select_function_works(started_cluster):
    sqlite_setup = node1.odbc_drivers["SQLite3"]
    sqlite_db = sqlite_setup["Database"]

    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t1 values(1, 2, 3);' | sqlite3 {}".format(sqlite_db)],
                            privileged=True, user='root')
    assert node1.query("select * from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "1\t2\t3\n"

    assert node1.query("select y from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "2\n"
    assert node1.query("select z from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "3\n"
    assert node1.query("select x from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "1\n"
    assert node1.query("select x, y from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "1\t2\n"
    assert node1.query("select z, x, y from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "3\t1\t2\n"
    assert node1.query(
        "select count(), sum(x) from odbc('DSN={}', '{}') group by x".format(sqlite_setup["DSN"], 't1')) == "1\t1\n"


def test_sqlite_simple_select_storage_works(started_cluster):
    sqlite_setup = node1.odbc_drivers["SQLite3"]
    sqlite_db = sqlite_setup["Database"]

    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t4 values(1, 2, 3);' | sqlite3 {}".format(sqlite_db)],
                            privileged=True, user='root')
    node1.query("create table SqliteODBC (x Int32, y String, z String) engine = ODBC('DSN={}', '', 't4')".format(
        sqlite_setup["DSN"]))

    assert node1.query("select * from SqliteODBC") == "1\t2\t3\n"
    assert node1.query("select y from SqliteODBC") == "2\n"
    assert node1.query("select z from SqliteODBC") == "3\n"
    assert node1.query("select x from SqliteODBC") == "1\n"
    assert node1.query("select x, y from SqliteODBC") == "1\t2\n"
    assert node1.query("select z, x, y from SqliteODBC") == "3\t1\t2\n"
    assert node1.query("select count(), sum(x) from SqliteODBC group by x") == "1\t1\n"


def test_sqlite_odbc_hashed_dictionary(started_cluster):
    sqlite_db = node1.odbc_drivers["SQLite3"]["Database"]
    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t2 values(1, 2, 3);' | sqlite3 {}".format(sqlite_db)],
                            privileged=True, user='root')

    node1.query("SYSTEM RELOAD DICTIONARY sqlite3_odbc_hashed")
    first_update_time = node1.query("SELECT last_successful_update_time FROM system.dictionaries WHERE name = 'sqlite3_odbc_hashed'")
    print("First update time", first_update_time)

    assert_eq_with_retry(node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(1))", "3")
    assert_eq_with_retry(node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(200))", "1")  # default

    second_update_time = node1.query("SELECT last_successful_update_time FROM system.dictionaries WHERE name = 'sqlite3_odbc_hashed'")
    # Reloaded with new data
    print("Second update time", second_update_time)
    while first_update_time == second_update_time:
        second_update_time = node1.query("SELECT last_successful_update_time FROM system.dictionaries WHERE name = 'sqlite3_odbc_hashed'")
        print("Waiting dictionary to update for the second time")
        time.sleep(0.1)

    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t2 values(200, 2, 7);' | sqlite3 {}".format(sqlite_db)],
                            privileged=True, user='root')

    # No reload because of invalidate query
    third_update_time = node1.query("SELECT last_successful_update_time FROM system.dictionaries WHERE name = 'sqlite3_odbc_hashed'")
    print("Third update time", second_update_time)
    counter = 0
    while third_update_time == second_update_time:
        third_update_time = node1.query("SELECT last_successful_update_time FROM system.dictionaries WHERE name = 'sqlite3_odbc_hashed'")
        time.sleep(0.1)
        if counter > 50:
            break
        counter += 1

    assert_eq_with_retry(node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(1))", "3")
    assert_eq_with_retry(node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(200))", "1") # still default

    node1.exec_in_container(["bash", "-c", "echo 'REPLACE INTO t2 values(1, 2, 5);' | sqlite3 {}".format(sqlite_db)],
                            privileged=True, user='root')

    assert_eq_with_retry(node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(1))", "5")
    assert_eq_with_retry(node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(200))", "7")


def test_sqlite_odbc_cached_dictionary(started_cluster):
    sqlite_db = node1.odbc_drivers["SQLite3"]["Database"]
    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t3 values(1, 2, 3);' | sqlite3 {}".format(sqlite_db)],
                            privileged=True, user='root')

    assert node1.query("select dictGetUInt8('sqlite3_odbc_cached', 'Z', toUInt64(1))") == "3\n"

    # Allow insert
    node1.exec_in_container(["bash", "-c", "chmod a+rw /tmp"], privileged=True, user='root')
    node1.exec_in_container(["bash", "-c", "chmod a+rw {}".format(sqlite_db)], privileged=True, user='root')

    node1.query("insert into table function odbc('DSN={};', '', 't3') values (200, 2, 7)".format(
        node1.odbc_drivers["SQLite3"]["DSN"]))

    assert node1.query("select dictGetUInt8('sqlite3_odbc_cached', 'Z', toUInt64(200))") == "7\n"  # new value

    node1.exec_in_container(["bash", "-c", "echo 'REPLACE INTO t3 values(1, 2, 12);' | sqlite3 {}".format(sqlite_db)],
                            privileged=True, user='root')

    assert_eq_with_retry(node1, "select dictGetUInt8('sqlite3_odbc_cached', 'Z', toUInt64(1))", "12")


def test_postgres_odbc_hached_dictionary_with_schema(started_cluster):
    conn = get_postgres_conn()
    cursor = conn.cursor()
    cursor.execute("insert into clickhouse.test_table values(1, 'hello'),(2, 'world')")
    node1.query("SYSTEM RELOAD DICTIONARY postgres_odbc_hashed")
    assert_eq_with_retry(node1, "select dictGetString('postgres_odbc_hashed', 'column2', toUInt64(1))", "hello")
    assert_eq_with_retry(node1, "select dictGetString('postgres_odbc_hashed', 'column2', toUInt64(2))", "world")


def test_postgres_odbc_hached_dictionary_no_tty_pipe_overflow(started_cluster):
    conn = get_postgres_conn()
    cursor = conn.cursor()
    cursor.execute("insert into clickhouse.test_table values(3, 'xxx')")
    for i in range(100):
        try:
            node1.query("system reload dictionary postgres_odbc_hashed", timeout=5)
        except Exception as ex:
            assert False, "Exception occured -- odbc-bridge hangs: " + str(ex)

    assert_eq_with_retry(node1, "select dictGetString('postgres_odbc_hashed', 'column2', toUInt64(3))", "xxx")


def test_postgres_insert(started_cluster):
    conn = get_postgres_conn()
    conn.cursor().execute("truncate table clickhouse.test_table")

    # Also test with Servername containing '.' and '-' symbols (defined in
    # postgres .yml file). This is needed to check parsing, validation and
    # reconstruction of connection string.

    node1.query(
        "create table pg_insert (column1 UInt8, column2 String) engine=ODBC('DSN=postgresql_odbc;Servername=postgre-sql.local', 'clickhouse', 'test_table')")
    node1.query("insert into pg_insert values (1, 'hello'), (2, 'world')")
    assert node1.query("select * from pg_insert") == '1\thello\n2\tworld\n'
    node1.query("insert into table function odbc('DSN=postgresql_odbc;', 'clickhouse', 'test_table') format CSV 3,test")
    node1.query(
        "insert into table function odbc('DSN=postgresql_odbc;Servername=postgre-sql.local', 'clickhouse', 'test_table') select number, 's' || toString(number) from numbers (4, 7)")
    assert node1.query("select sum(column1), count(column1) from pg_insert") == "55\t10\n"
    assert node1.query(
        "select sum(n), count(n) from (select (*,).1 as n from (select * from odbc('DSN=postgresql_odbc;', 'clickhouse', 'test_table')))") == "55\t10\n"


def test_bridge_dies_with_parent(started_cluster):
    if node1.is_built_with_address_sanitizer():
        # TODO: Leak sanitizer falsely reports about a leak of 16 bytes in clickhouse-odbc-bridge in this test and
        # that's linked somehow with that we have replaced getauxval() in glibc-compatibility.
        # The leak sanitizer calls getauxval() for its own purposes, and our replaced version doesn't seem to be equivalent in that case.
        return

    node1.query("select dictGetString('postgres_odbc_hashed', 'column2', toUInt64(1))")

    clickhouse_pid = node1.get_process_pid("clickhouse server")
    bridge_pid = node1.get_process_pid("odbc-bridge")
    assert clickhouse_pid is not None
    assert bridge_pid is not None

    while clickhouse_pid is not None:
        try:
            node1.exec_in_container(["bash", "-c", "kill {}".format(clickhouse_pid)], privileged=True, user='root')
        except:
            pass
        clickhouse_pid = node1.get_process_pid("clickhouse server")
        time.sleep(1)

    for i in range(30):
        time.sleep(1)  # just for sure, that odbc-bridge caught signal
        bridge_pid = node1.get_process_pid("odbc-bridge")
        if bridge_pid is None:
            break

    if bridge_pid:
        out = node1.exec_in_container(["gdb", "-p", str(bridge_pid), "--ex", "thread apply all bt", "--ex", "q"],
                                      privileged=True, user='root')
        print("Bridge is running, gdb output:")
        print(out)

    assert clickhouse_pid is None
    assert bridge_pid is None
