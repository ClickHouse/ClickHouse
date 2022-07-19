import time

import psycopg2
import pymysql.cursors
import pytest
import logging
import os.path

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from multiprocessing.dummy import Pool

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    with_odbc_drivers=True,
    with_mysql=True,
    with_postgres=True,
    main_configs=["configs/openssl.xml", "configs/odbc_logging.xml"],
    dictionaries=[
        "configs/dictionaries/sqlite3_odbc_hashed_dictionary.xml",
        "configs/dictionaries/sqlite3_odbc_cached_dictionary.xml",
        "configs/dictionaries/postgres_odbc_hashed_dictionary.xml",
    ],
    stay_alive=True,
)


drop_table_sql_template = """
    DROP TABLE IF EXISTS `clickhouse`.`{}`
    """

create_table_sql_template = """
    CREATE TABLE `clickhouse`.`{}` (
    `id` int(11) NOT NULL,
    `name` varchar(50) NOT NULL,
    `age` int  NOT NULL default 0,
    `money` int NOT NULL default 0,
    `column_x` int default NULL,
    PRIMARY KEY (`id`)) ENGINE=InnoDB;
    """


def skip_test_msan(instance):
    if instance.is_built_with_memory_sanitizer():
        pytest.skip("Memory Sanitizer cannot work with third-party shared libraries")


def get_mysql_conn():
    errors = []
    conn = None
    for _ in range(15):
        try:
            if conn is None:
                conn = pymysql.connect(
                    user="root",
                    password="clickhouse",
                    host=cluster.mysql_ip,
                    port=cluster.mysql_port,
                )
            else:
                conn.ping(reconnect=True)
            logging.debug(
                f"MySQL Connection establised: {cluster.mysql_ip}:{cluster.mysql_port}"
            )
            return conn
        except Exception as e:
            errors += [str(e)]
            time.sleep(1)

    raise Exception("Connection not establised, {}".format(errors))


def create_mysql_db(conn, name):
    with conn.cursor() as cursor:
        cursor.execute("DROP DATABASE IF EXISTS {}".format(name))
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))


def create_mysql_table(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(drop_table_sql_template.format(table_name))
        cursor.execute(create_table_sql_template.format(table_name))


def get_postgres_conn(started_cluster):
    conn_string = "host={} port={} user='postgres' password='mysecretpassword'".format(
        started_cluster.postgres_ip, started_cluster.postgres_port
    )
    errors = []
    for _ in range(15):
        try:
            conn = psycopg2.connect(conn_string)
            logging.debug("Postgre Connection establised: {}".format(conn_string))
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            conn.autocommit = True
            return conn
        except Exception as e:
            errors += [str(e)]
            time.sleep(1)

    raise Exception(
        "Postgre connection not establised DSN={}, {}".format(conn_string, errors)
    )


def create_postgres_db(conn, name):
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA {}".format(name))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        sqlite_db = node1.odbc_drivers["SQLite3"]["Database"]

        logging.debug(f"sqlite data received: {sqlite_db}")
        node1.exec_in_container(
            [
                "sqlite3",
                sqlite_db,
                "CREATE TABLE t1(id INTEGER PRIMARY KEY ASC, x INTEGER, y, z);",
            ],
            privileged=True,
            user="root",
        )
        node1.exec_in_container(
            [
                "sqlite3",
                sqlite_db,
                "CREATE TABLE t2(id INTEGER PRIMARY KEY ASC, X INTEGER, Y, Z);",
            ],
            privileged=True,
            user="root",
        )
        node1.exec_in_container(
            [
                "sqlite3",
                sqlite_db,
                "CREATE TABLE t3(id INTEGER PRIMARY KEY ASC, X INTEGER, Y, Z);",
            ],
            privileged=True,
            user="root",
        )
        node1.exec_in_container(
            [
                "sqlite3",
                sqlite_db,
                "CREATE TABLE t4(id INTEGER PRIMARY KEY ASC, X INTEGER, Y, Z);",
            ],
            privileged=True,
            user="root",
        )
        node1.exec_in_container(
            [
                "sqlite3",
                sqlite_db,
                "CREATE TABLE tf1(id INTEGER PRIMARY KEY ASC, x INTEGER, y, z);",
            ],
            privileged=True,
            user="root",
        )
        logging.debug("sqlite tables created")
        mysql_conn = get_mysql_conn()
        logging.debug("mysql connection received")
        ## create mysql db and table
        create_mysql_db(mysql_conn, "clickhouse")
        logging.debug("mysql database created")

        postgres_conn = get_postgres_conn(cluster)
        logging.debug("postgres connection received")

        create_postgres_db(postgres_conn, "clickhouse")
        logging.debug("postgres db created")

        cursor = postgres_conn.cursor()
        cursor.execute(
            "create table if not exists clickhouse.test_table (id int primary key, column1 int not null, column2 varchar(40) not null)"
        )

        yield cluster

    except Exception as ex:
        logging.exception(ex)
        raise ex
    finally:
        cluster.shutdown()


def test_mysql_simple_select_works(started_cluster):
    skip_test_msan(node1)

    mysql_setup = node1.odbc_drivers["MySQL"]

    table_name = "test_insert_select"
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    # Check that NULL-values are handled correctly by the ODBC-bridge
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO clickhouse.{} VALUES(50, 'null-guy', 127, 255, NULL), (100, 'non-null-guy', 127, 255, 511);".format(
                table_name
            )
        )
        conn.commit()
    assert (
        node1.query(
            "SELECT column_x FROM odbc('DSN={}', '{}')".format(
                mysql_setup["DSN"], table_name
            ),
            settings={"external_table_functions_use_nulls": "1"},
        )
        == "\\N\n511\n"
    )
    assert (
        node1.query(
            "SELECT column_x FROM odbc('DSN={}', '{}')".format(
                mysql_setup["DSN"], table_name
            ),
            settings={"external_table_functions_use_nulls": "0"},
        )
        == "0\n511\n"
    )

    node1.query(
        """
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32, column_x Nullable(UInt32)) ENGINE = MySQL('mysql57:3306', 'clickhouse', '{}', 'root', 'clickhouse');
""".format(
            table_name, table_name
        )
    )

    node1.query(
        "INSERT INTO {}(id, name, money, column_x) select number, concat('name_', toString(number)), 3, NULL from numbers(49) ".format(
            table_name
        )
    )
    node1.query(
        "INSERT INTO {}(id, name, money, column_x) select number, concat('name_', toString(number)), 3, 42 from numbers(51, 49) ".format(
            table_name
        )
    )

    assert (
        node1.query(
            "SELECT COUNT () FROM {} WHERE column_x IS NOT NULL".format(table_name)
        )
        == "50\n"
    )
    assert (
        node1.query("SELECT COUNT () FROM {} WHERE column_x IS NULL".format(table_name))
        == "50\n"
    )
    assert (
        node1.query(
            "SELECT count(*) FROM odbc('DSN={}', '{}')".format(
                mysql_setup["DSN"], table_name
            )
        )
        == "100\n"
    )

    # previously this test fails with segfault
    # just to be sure :)
    assert node1.query("select 1") == "1\n"

    conn.close()


def test_mysql_insert(started_cluster):
    skip_test_msan(node1)

    mysql_setup = node1.odbc_drivers["MySQL"]
    table_name = "test_insert"
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)
    odbc_args = "'DSN={}', '{}', '{}'".format(
        mysql_setup["DSN"], mysql_setup["Database"], table_name
    )

    node1.query(
        "create table mysql_insert (id Int64, name String, age UInt8, money Float, column_x Nullable(Int16)) Engine=ODBC({})".format(
            odbc_args
        )
    )
    node1.query(
        "insert into mysql_insert values (1, 'test', 11, 111, 1111), (2, 'odbc', 22, 222, NULL)"
    )
    assert (
        node1.query("select * from mysql_insert")
        == "1\ttest\t11\t111\t1111\n2\todbc\t22\t222\t\\N\n"
    )

    node1.query(
        "insert into table function odbc({}) values (3, 'insert', 33, 333, 3333)".format(
            odbc_args
        )
    )
    node1.query(
        "insert into table function odbc({}) (id, name, age, money) select id*4, upper(name), age*4, money*4 from odbc({}) where id=1".format(
            odbc_args, odbc_args
        )
    )
    assert (
        node1.query("select * from mysql_insert where id in (3, 4)")
        == "3\tinsert\t33\t333\t3333\n4\tTEST\t44\t444\t\\N\n"
    )


def test_sqlite_simple_select_function_works(started_cluster):
    skip_test_msan(node1)

    sqlite_setup = node1.odbc_drivers["SQLite3"]
    sqlite_db = sqlite_setup["Database"]

    node1.exec_in_container(
        ["sqlite3", sqlite_db, "INSERT INTO t1 values(1, 1, 2, 3);"],
        privileged=True,
        user="root",
    )
    assert (
        node1.query(
            "select * from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], "t1")
        )
        == "1\t1\t2\t3\n"
    )

    assert (
        node1.query(
            "select y from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], "t1")
        )
        == "2\n"
    )
    assert (
        node1.query(
            "select z from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], "t1")
        )
        == "3\n"
    )
    assert (
        node1.query(
            "select x from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], "t1")
        )
        == "1\n"
    )
    assert (
        node1.query(
            "select x, y from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], "t1")
        )
        == "1\t2\n"
    )
    assert (
        node1.query(
            "select z, x, y from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], "t1")
        )
        == "3\t1\t2\n"
    )
    assert (
        node1.query(
            "select count(), sum(x) from odbc('DSN={}', '{}') group by x".format(
                sqlite_setup["DSN"], "t1"
            )
        )
        == "1\t1\n"
    )


def test_sqlite_table_function(started_cluster):
    skip_test_msan(node1)

    sqlite_setup = node1.odbc_drivers["SQLite3"]
    sqlite_db = sqlite_setup["Database"]

    node1.exec_in_container(
        ["sqlite3", sqlite_db, "INSERT INTO tf1 values(1, 1, 2, 3);"],
        privileged=True,
        user="root",
    )
    node1.query(
        "create table odbc_tf as odbc('DSN={}', '{}')".format(
            sqlite_setup["DSN"], "tf1"
        )
    )
    assert node1.query("select * from odbc_tf") == "1\t1\t2\t3\n"

    assert node1.query("select y from odbc_tf") == "2\n"
    assert node1.query("select z from odbc_tf") == "3\n"
    assert node1.query("select x from odbc_tf") == "1\n"
    assert node1.query("select x, y from odbc_tf") == "1\t2\n"
    assert node1.query("select z, x, y from odbc_tf") == "3\t1\t2\n"
    assert node1.query("select count(), sum(x) from odbc_tf group by x") == "1\t1\n"


def test_sqlite_simple_select_storage_works(started_cluster):
    skip_test_msan(node1)

    sqlite_setup = node1.odbc_drivers["SQLite3"]
    sqlite_db = sqlite_setup["Database"]

    node1.exec_in_container(
        ["sqlite3", sqlite_db, "INSERT INTO t4 values(1, 1, 2, 3);"],
        privileged=True,
        user="root",
    )
    node1.query(
        "create table SqliteODBC (x Int32, y String, z String) engine = ODBC('DSN={}', '', 't4')".format(
            sqlite_setup["DSN"]
        )
    )

    assert node1.query("select * from SqliteODBC") == "1\t2\t3\n"
    assert node1.query("select y from SqliteODBC") == "2\n"
    assert node1.query("select z from SqliteODBC") == "3\n"
    assert node1.query("select x from SqliteODBC") == "1\n"
    assert node1.query("select x, y from SqliteODBC") == "1\t2\n"
    assert node1.query("select z, x, y from SqliteODBC") == "3\t1\t2\n"
    assert node1.query("select count(), sum(x) from SqliteODBC group by x") == "1\t1\n"


def test_sqlite_odbc_hashed_dictionary(started_cluster):
    skip_test_msan(node1)

    sqlite_db = node1.odbc_drivers["SQLite3"]["Database"]
    node1.exec_in_container(
        ["sqlite3", sqlite_db, "INSERT INTO t2 values(1, 1, 2, 3);"],
        privileged=True,
        user="root",
    )

    node1.query("SYSTEM RELOAD DICTIONARY sqlite3_odbc_hashed")
    first_update_time = node1.query(
        "SELECT last_successful_update_time FROM system.dictionaries WHERE name = 'sqlite3_odbc_hashed'"
    )
    logging.debug(f"First update time {first_update_time}")

    assert_eq_with_retry(
        node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(1))", "3"
    )
    assert_eq_with_retry(
        node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(200))", "1"
    )  # default

    second_update_time = node1.query(
        "SELECT last_successful_update_time FROM system.dictionaries WHERE name = 'sqlite3_odbc_hashed'"
    )
    # Reloaded with new data
    logging.debug(f"Second update time {second_update_time}")
    while first_update_time == second_update_time:
        second_update_time = node1.query(
            "SELECT last_successful_update_time FROM system.dictionaries WHERE name = 'sqlite3_odbc_hashed'"
        )
        logging.debug("Waiting dictionary to update for the second time")
        time.sleep(0.1)

    node1.exec_in_container(
        ["sqlite3", sqlite_db, "INSERT INTO t2 values(200, 200, 2, 7);"],
        privileged=True,
        user="root",
    )

    # No reload because of invalidate query
    third_update_time = node1.query(
        "SELECT last_successful_update_time FROM system.dictionaries WHERE name = 'sqlite3_odbc_hashed'"
    )
    logging.debug(f"Third update time {second_update_time}")
    counter = 0
    while third_update_time == second_update_time:
        third_update_time = node1.query(
            "SELECT last_successful_update_time FROM system.dictionaries WHERE name = 'sqlite3_odbc_hashed'"
        )
        time.sleep(0.1)
        if counter > 50:
            break
        counter += 1

    assert_eq_with_retry(
        node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(1))", "3"
    )
    assert_eq_with_retry(
        node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(200))", "1"
    )  # still default

    node1.exec_in_container(
        ["sqlite3", sqlite_db, "REPLACE INTO t2 values(1, 1, 2, 5);"],
        privileged=True,
        user="root",
    )

    assert_eq_with_retry(
        node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(1))", "5"
    )
    assert_eq_with_retry(
        node1, "select dictGetUInt8('sqlite3_odbc_hashed', 'Z', toUInt64(200))", "7"
    )


def test_sqlite_odbc_cached_dictionary(started_cluster):
    skip_test_msan(node1)

    sqlite_db = node1.odbc_drivers["SQLite3"]["Database"]
    node1.exec_in_container(
        ["sqlite3", sqlite_db, "INSERT INTO t3 values(1, 1, 2, 3);"],
        privileged=True,
        user="root",
    )

    assert (
        node1.query("select dictGetUInt8('sqlite3_odbc_cached', 'Z', toUInt64(1))")
        == "3\n"
    )

    # Allow insert
    node1.exec_in_container(["chmod", "a+rw", "/tmp"], privileged=True, user="root")
    node1.exec_in_container(["chmod", "a+rw", sqlite_db], privileged=True, user="root")

    node1.query(
        "insert into table function odbc('DSN={};ReadOnly=0', '', 't3') values (200, 200, 2, 7)".format(
            node1.odbc_drivers["SQLite3"]["DSN"]
        )
    )

    assert (
        node1.query("select dictGetUInt8('sqlite3_odbc_cached', 'Z', toUInt64(200))")
        == "7\n"
    )  # new value

    node1.exec_in_container(
        ["sqlite3", sqlite_db, "REPLACE INTO t3 values(1, 1, 2, 12);"],
        privileged=True,
        user="root",
    )

    assert_eq_with_retry(
        node1, "select dictGetUInt8('sqlite3_odbc_cached', 'Z', toUInt64(1))", "12"
    )


def test_postgres_odbc_hashed_dictionary_with_schema(started_cluster):
    skip_test_msan(node1)

    conn = get_postgres_conn(started_cluster)
    cursor = conn.cursor()
    cursor.execute("truncate table clickhouse.test_table")
    cursor.execute(
        "insert into clickhouse.test_table values(1, 1, 'hello'),(2, 2, 'world')"
    )
    node1.query("SYSTEM RELOAD DICTIONARY postgres_odbc_hashed")
    node1.exec_in_container(
        ["ss", "-K", "dport", "postgresql"], privileged=True, user="root"
    )
    node1.query("SYSTEM RELOAD DICTIONARY postgres_odbc_hashed")
    assert_eq_with_retry(
        node1,
        "select dictGetString('postgres_odbc_hashed', 'column2', toUInt64(1))",
        "hello",
    )
    assert_eq_with_retry(
        node1,
        "select dictGetString('postgres_odbc_hashed', 'column2', toUInt64(2))",
        "world",
    )


def test_postgres_odbc_hashed_dictionary_no_tty_pipe_overflow(started_cluster):
    skip_test_msan(node1)

    conn = get_postgres_conn(started_cluster)
    cursor = conn.cursor()
    cursor.execute("truncate table clickhouse.test_table")
    cursor.execute("insert into clickhouse.test_table values(3, 3, 'xxx')")
    for i in range(100):
        try:
            node1.query("system reload dictionary postgres_odbc_hashed", timeout=15)
        except Exception as ex:
            assert False, "Exception occured -- odbc-bridge hangs: " + str(ex)

    assert_eq_with_retry(
        node1,
        "select dictGetString('postgres_odbc_hashed', 'column2', toUInt64(3))",
        "xxx",
    )


def test_postgres_insert(started_cluster):
    skip_test_msan(node1)

    conn = get_postgres_conn(started_cluster)
    conn.cursor().execute("truncate table clickhouse.test_table")

    # Also test with Servername containing '.' and '-' symbols (defined in
    # postgres .yml file). This is needed to check parsing, validation and
    # reconstruction of connection string.

    node1.query(
        "create table pg_insert (id UInt64, column1 UInt8, column2 String) engine=ODBC('DSN=postgresql_odbc;Servername=postgre-sql.local', 'clickhouse', 'test_table')"
    )
    node1.query("insert into pg_insert values (1, 1, 'hello'), (2, 2, 'world')")
    assert node1.query("select * from pg_insert") == "1\t1\thello\n2\t2\tworld\n"
    node1.query(
        "insert into table function odbc('DSN=postgresql_odbc', 'clickhouse', 'test_table') format CSV 3,3,test"
    )
    node1.query(
        "insert into table function odbc('DSN=postgresql_odbc;Servername=postgre-sql.local', 'clickhouse', 'test_table')"
        " select number, number, 's' || toString(number) from numbers (4, 7)"
    )
    assert (
        node1.query("select sum(column1), count(column1) from pg_insert") == "55\t10\n"
    )
    assert (
        node1.query(
            "select sum(n), count(n) from (select (*,).1 as n from (select * from odbc('DSN=postgresql_odbc', 'clickhouse', 'test_table')))"
        )
        == "55\t10\n"
    )


def test_bridge_dies_with_parent(started_cluster):
    skip_test_msan(node1)

    if node1.is_built_with_address_sanitizer():
        # TODO: Leak sanitizer falsely reports about a leak of 16 bytes in clickhouse-odbc-bridge in this test and
        # that's linked somehow with that we have replaced getauxval() in glibc-compatibility.
        # The leak sanitizer calls getauxval() for its own purposes, and our replaced version doesn't seem to be equivalent in that case.
        pytest.skip(
            "Leak sanitizer falsely reports about a leak of 16 bytes in clickhouse-odbc-bridge"
        )

    node1.query("select dictGetString('postgres_odbc_hashed', 'column2', toUInt64(1))")

    clickhouse_pid = node1.get_process_pid("clickhouse server")
    bridge_pid = node1.get_process_pid("odbc-bridge")
    assert clickhouse_pid is not None
    assert bridge_pid is not None

    while clickhouse_pid is not None:
        try:
            node1.exec_in_container(
                ["kill", str(clickhouse_pid)], privileged=True, user="root"
            )
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
        out = node1.exec_in_container(
            ["gdb", "-p", str(bridge_pid), "--ex", "thread apply all bt", "--ex", "q"],
            privileged=True,
            user="root",
        )
        logging.debug(f"Bridge is running, gdb output:\n{out}")

    assert clickhouse_pid is None
    assert bridge_pid is None
    node1.start_clickhouse(20)


def test_odbc_postgres_date_data_type(started_cluster):
    skip_test_msan(node1)

    conn = get_postgres_conn(started_cluster)
    cursor = conn.cursor()
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS clickhouse.test_date (id integer, column1 integer, column2 date)"
    )

    cursor.execute("INSERT INTO clickhouse.test_date VALUES (1, 1, '2020-12-01')")
    cursor.execute("INSERT INTO clickhouse.test_date VALUES (2, 2, '2020-12-02')")
    cursor.execute("INSERT INTO clickhouse.test_date VALUES (3, 3, '2020-12-03')")
    conn.commit()

    node1.query(
        """
        CREATE TABLE test_date (id UInt64, column1 UInt64, column2 Date)
        ENGINE=ODBC('DSN=postgresql_odbc; Servername=postgre-sql.local', 'clickhouse', 'test_date')"""
    )

    expected = "1\t1\t2020-12-01\n2\t2\t2020-12-02\n3\t3\t2020-12-03\n"
    result = node1.query("SELECT * FROM test_date")
    assert result == expected
    cursor.execute("DROP TABLE IF EXISTS clickhouse.test_date")
    node1.query("DROP TABLE IF EXISTS test_date")


def test_odbc_postgres_conversions(started_cluster):
    skip_test_msan(node1)

    conn = get_postgres_conn(started_cluster)
    cursor = conn.cursor()

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS clickhouse.test_types (
        a smallint, b integer, c bigint, d real, e double precision, f serial, g bigserial,
        h timestamp)"""
    )

    node1.query(
        """
        INSERT INTO TABLE FUNCTION
        odbc('DSN=postgresql_odbc; Servername=postgre-sql.local', 'clickhouse', 'test_types')
        VALUES (-32768, -2147483648, -9223372036854775808, 1.12345, 1.1234567890, 2147483647, 9223372036854775807, '2000-05-12 12:12:12')"""
    )

    result = node1.query(
        """
        SELECT a, b, c, d, e, f, g, h
        FROM odbc('DSN=postgresql_odbc; Servername=postgre-sql.local', 'clickhouse', 'test_types')
        """
    )

    assert (
        result
        == "-32768\t-2147483648\t-9223372036854775808\t1.12345\t1.123456789\t2147483647\t9223372036854775807\t2000-05-12 12:12:12\n"
    )
    cursor.execute("DROP TABLE IF EXISTS clickhouse.test_types")

    cursor.execute(
        """CREATE TABLE IF NOT EXISTS clickhouse.test_types (column1 Timestamp, column2 Numeric)"""
    )

    node1.query(
        """
        CREATE TABLE test_types (column1 DateTime64, column2 Decimal(5, 1))
        ENGINE=ODBC('DSN=postgresql_odbc; Servername=postgre-sql.local', 'clickhouse', 'test_types')"""
    )

    node1.query(
        """INSERT INTO test_types
        SELECT toDateTime64('2019-01-01 00:00:00', 3, 'Etc/UTC'), toDecimal32(1.1, 1)"""
    )

    expected = node1.query(
        "SELECT toDateTime64('2019-01-01 00:00:00', 3, 'Etc/UTC'), toDecimal32(1.1, 1)"
    )
    result = node1.query("SELECT * FROM test_types")
    logging.debug(result)
    cursor.execute("DROP TABLE IF EXISTS clickhouse.test_types")
    assert result == expected


def test_odbc_cyrillic_with_varchar(started_cluster):
    skip_test_msan(node1)

    conn = get_postgres_conn(started_cluster)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS clickhouse.test_cyrillic")
    cursor.execute("CREATE TABLE clickhouse.test_cyrillic (name varchar(11))")

    node1.query(
        """
        CREATE TABLE test_cyrillic (name String)
        ENGINE = ODBC('DSN=postgresql_odbc; Servername=postgre-sql.local', 'clickhouse', 'test_cyrillic')"""
    )

    cursor.execute("INSERT INTO clickhouse.test_cyrillic VALUES ('A-nice-word')")
    cursor.execute("INSERT INTO clickhouse.test_cyrillic VALUES ('Красивенько')")

    result = node1.query(""" SELECT * FROM test_cyrillic ORDER BY name""")
    assert result == "A-nice-word\nКрасивенько\n"
    result = node1.query(
        """ SELECT name FROM odbc('DSN=postgresql_odbc; Servername=postgre-sql.local', 'clickhouse', 'test_cyrillic') """
    )
    assert result == "A-nice-word\nКрасивенько\n"


def test_many_connections(started_cluster):
    skip_test_msan(node1)

    conn = get_postgres_conn(started_cluster)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS clickhouse.test_pg_table")
    cursor.execute("CREATE TABLE clickhouse.test_pg_table (key integer, value integer)")

    node1.query(
        """
        DROP TABLE IF EXISTS test_pg_table;
        CREATE TABLE test_pg_table (key UInt32, value UInt32)
        ENGINE = ODBC('DSN=postgresql_odbc; Servername=postgre-sql.local', 'clickhouse', 'test_pg_table')"""
    )

    node1.query("INSERT INTO test_pg_table SELECT number, number FROM numbers(10)")

    query = "SELECT count() FROM ("
    for i in range(24):
        query += "SELECT key FROM {t} UNION ALL "
    query += "SELECT key FROM {t})"

    assert node1.query(query.format(t="test_pg_table")) == "250\n"


def test_concurrent_queries(started_cluster):
    skip_test_msan(node1)

    conn = get_postgres_conn(started_cluster)
    cursor = conn.cursor()

    node1.query(
        """
        DROP TABLE IF EXISTS test_pg_table;
        CREATE TABLE test_pg_table (key UInt32, value UInt32)
        ENGINE = ODBC('DSN=postgresql_odbc; Servername=postgre-sql.local', 'clickhouse', 'test_pg_table')"""
    )

    cursor.execute("DROP TABLE IF EXISTS clickhouse.test_pg_table")
    cursor.execute("CREATE TABLE clickhouse.test_pg_table (key integer, value integer)")

    def node_insert(_):
        for i in range(5):
            node1.query(
                "INSERT INTO test_pg_table SELECT number, number FROM numbers(1000)",
                user="default",
            )

    busy_pool = Pool(5)
    p = busy_pool.map_async(node_insert, range(5))
    p.wait()
    assert_eq_with_retry(
        node1, "SELECT count() FROM test_pg_table", str(5 * 5 * 1000), retry_count=100
    )

    def node_insert_select(_):
        for i in range(5):
            result = node1.query(
                "INSERT INTO test_pg_table SELECT number, number FROM numbers(1000)",
                user="default",
            )
            result = node1.query(
                "SELECT * FROM test_pg_table LIMIT 100", user="default"
            )

    busy_pool = Pool(5)
    p = busy_pool.map_async(node_insert_select, range(5))
    p.wait()
    assert_eq_with_retry(
        node1,
        "SELECT count() FROM test_pg_table",
        str(5 * 5 * 1000 * 2),
        retry_count=100,
    )

    node1.query("DROP TABLE test_pg_table;")
    cursor.execute("DROP TABLE clickhouse.test_pg_table;")


def test_odbc_long_column_names(started_cluster):
    skip_test_msan(node1)

    conn = get_postgres_conn(started_cluster)
    cursor = conn.cursor()

    column_name = "column" * 8
    create_table = "CREATE TABLE clickhouse.test_long_column_names ("
    for i in range(1000):
        if i != 0:
            create_table += ", "
        create_table += "{} integer".format(column_name + str(i))
    create_table += ")"
    cursor.execute(create_table)
    insert = (
        "INSERT INTO clickhouse.test_long_column_names SELECT i"
        + ", i" * 999
        + " FROM generate_series(0, 99) as t(i)"
    )
    cursor.execute(insert)
    conn.commit()

    create_table = "CREATE TABLE test_long_column_names ("
    for i in range(1000):
        if i != 0:
            create_table += ", "
        create_table += "{} UInt32".format(column_name + str(i))
    create_table += ") ENGINE=ODBC('DSN=postgresql_odbc; Servername=postgre-sql.local', 'clickhouse', 'test_long_column_names')"
    result = node1.query(create_table)

    result = node1.query("SELECT * FROM test_long_column_names")
    expected = node1.query("SELECT number" + ", number" * 999 + " FROM numbers(100)")
    assert result == expected

    cursor.execute("DROP TABLE IF EXISTS clickhouse.test_long_column_names")
    node1.query("DROP TABLE IF EXISTS test_long_column_names")


def test_odbc_long_text(started_cluster):
    skip_test_msan(node1)

    conn = get_postgres_conn(started_cluster)
    cursor = conn.cursor()
    cursor.execute("drop table if exists clickhouse.test_long_text")
    cursor.execute("create table clickhouse.test_long_text(flen int, field1 text)")

    # sample test from issue 9363
    text_from_issue = """BEGIN These examples only show the order that data is arranged in. The values from different columns are stored separately, and data from the same column is stored together.      Examples of a column-oriented DBMS: Vertica, Paraccel (Actian Matrix and Amazon Redshift), Sybase IQ, Exasol, Infobright, InfiniDB, MonetDB (VectorWise and Actian Vector), LucidDB, SAP HANA, Google Dremel, Google PowerDrill, Druid, and kdb+.      Different orders for storing data are better suited to different scenarios. The data access scenario refers to what queries are made, how often, and in what proportion; how much data is read for each type of query – rows, columns, and bytes; the relationship between reading and updating data; the working size of the data and how locally it is used; whether transactions are used, and how isolated they are; requirements for data replication and logical integrity; requirements for latency and throughput for each type of query, and so on.      The higher the load on the system, the more important it is to customize the system set up to match the requirements of the usage scenario, and the more fine grained this customization becomes. There is no system that is equally well-suited to significantly different scenarios. If a system is adaptable to a wide set of scenarios, under a high load, the system will handle all the scenarios equally poorly, or will work well for just one or few of possible scenarios.   Key Properties of OLAP Scenario¶          The vast majority of requests are for read access.       Data is updated in fairly large batches (> 1000 rows), not by single rows; or it is not updated at all.       Data is added to the DB but is not modified.       For reads, quite a large number of rows are extracted from the DB, but only a small subset of columns.       Tables are "wide," meaning they contain a large number of columns.       Queries are relatively rare (usually hundreds of queries per server or less per second).       For simple queries, latencies around 50 ms are allowed.       Column values are fairly small: numbers and short strings (for example, 60 bytes per URL).       Requires high throughput when processing a single query (up to billions of rows per second per server).       Transactions are not necessary.       Low requirements for data consistency.       There is one large table per query. All tables are small, except for one.       A query result is significantly smaller than the source data. In other words, data is filtered or aggregated, so the result fits in a single server"s RAM.      It is easy to see that the OLAP scenario is very different from other popular scenarios (such as OLTP or Key-Value access). So it doesn"t make sense to try to use OLTP or a Key-Value DB for processing analytical queries if you want to get decent performance. For example, if you try to use MongoDB or Redis for analytics, you will get very poor performance compared to OLAP databases.   Why Column-Oriented Databases Work Better in the OLAP Scenario¶      Column-oriented databases are better suited to OLAP scenarios: they are at least 100 times faster in processing most queries. The reasons are explained in detail below, but the fact is easier to demonstrate visually. END"""
    cursor.execute(
        """insert into clickhouse.test_long_text (flen, field1) values (3248, '{}')""".format(
            text_from_issue
        )
    )

    node1.query(
        """
        DROP TABLE IF EXISTS test_long_test;
        CREATE TABLE test_long_text (flen UInt32, field1 String)
        ENGINE = ODBC('DSN=postgresql_odbc; Servername=postgre-sql.local', 'clickhouse', 'test_long_text')"""
    )
    result = node1.query("select field1 from test_long_text;")
    assert result.strip() == text_from_issue

    long_text = "text" * 1000000
    cursor.execute(
        """insert into clickhouse.test_long_text (flen, field1) values (400000, '{}')""".format(
            long_text
        )
    )
    result = node1.query("select field1 from test_long_text where flen=400000;")
    assert result.strip() == long_text
