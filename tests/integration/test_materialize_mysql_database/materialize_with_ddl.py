import time

import pymysql.cursors
import pytest
from helpers.network import PartitionManager
import logging
from helpers.client import QueryRuntimeException
from helpers.cluster import get_docker_compose_path, run_and_check
import random

import threading
from multiprocessing.dummy import Pool
from helpers.test_tools import assert_eq_with_retry

def check_query(clickhouse_node, query, result_set, retry_count=10, interval_seconds=3):
    lastest_result = ''

    for i in range(retry_count):
        try:
            lastest_result = clickhouse_node.query(query)
            if result_set == lastest_result:
                return

            logging.debug(f"latest_result{lastest_result}")
            time.sleep(interval_seconds)
        except Exception as e:
            logging.debug(f"check_query retry {i+1} exception {e}")
            time.sleep(interval_seconds)
    else:
        assert clickhouse_node.query(query) == result_set


def dml_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_dml")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_dml")
    mysql_node.query("CREATE DATABASE test_database_dml DEFAULT CHARACTER SET 'utf8'")
    # existed before the mapping was created

    mysql_node.query("CREATE TABLE test_database_dml.test_table_1 ("
                     "`key` INT NOT NULL PRIMARY KEY, "
                     "unsigned_tiny_int TINYINT UNSIGNED, tiny_int TINYINT, "
                     "unsigned_small_int SMALLINT UNSIGNED, small_int SMALLINT, "
                     "unsigned_medium_int MEDIUMINT UNSIGNED, medium_int MEDIUMINT, "
                     "unsigned_int INT UNSIGNED, _int INT, "
                     "unsigned_integer INTEGER UNSIGNED, _integer INTEGER, "
                     "unsigned_bigint BIGINT UNSIGNED, _bigint BIGINT, "
                     "/* Need ClickHouse support read mysql decimal unsigned_decimal DECIMAL(19, 10) UNSIGNED, _decimal DECIMAL(19, 10), */"
                     "unsigned_float FLOAT UNSIGNED, _float FLOAT, "
                     "unsigned_double DOUBLE UNSIGNED, _double DOUBLE, "
                     "_varchar VARCHAR(10), _char CHAR(10), binary_col BINARY(8), "
                     "/* Need ClickHouse support Enum('a', 'b', 'v') _enum ENUM('a', 'b', 'c'), */"
                     "_date Date, _datetime DateTime, _timestamp TIMESTAMP, _bool BOOLEAN) ENGINE = InnoDB;")

    # it already has some data
    mysql_node.query("""
        INSERT INTO test_database_dml.test_table_1 VALUES(1, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 3.2, -3.2, 3.4, -3.4, 'varchar', 'char', 'binary',
        '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00', true);
        """)
    clickhouse_node.query(
        "CREATE DATABASE test_database_dml ENGINE = MaterializeMySQL('{}:3306', 'test_database_dml', 'root', 'clickhouse')".format(
            service_name))

    assert "test_database_dml" in clickhouse_node.query("SHOW DATABASES")

    check_query(clickhouse_node, "SELECT * FROM test_database_dml.test_table_1 ORDER BY key FORMAT TSV",
                "1\t1\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\tvarchar\tchar\tbinary\\0\\0\t2020-01-01\t"
                "2020-01-01 00:00:00\t2020-01-01 00:00:00\t1\n")

    mysql_node.query("""
        INSERT INTO test_database_dml.test_table_1 VALUES(2, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 3.2, -3.2, 3.4, -3.4, 'varchar', 'char', 'binary',
        '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00', false);
        """)

    check_query(clickhouse_node, "SELECT * FROM test_database_dml.test_table_1 ORDER BY key FORMAT TSV",
                "1\t1\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\tvarchar\tchar\tbinary\\0\\0\t2020-01-01\t"
                "2020-01-01 00:00:00\t2020-01-01 00:00:00\t1\n2\t1\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\t"
                "varchar\tchar\tbinary\\0\\0\t2020-01-01\t2020-01-01 00:00:00\t2020-01-01 00:00:00\t0\n")

    mysql_node.query("UPDATE test_database_dml.test_table_1 SET unsigned_tiny_int = 2 WHERE `key` = 1")

    check_query(clickhouse_node, """
        SELECT key, unsigned_tiny_int, tiny_int, unsigned_small_int,
         small_int, unsigned_medium_int, medium_int, unsigned_int, _int, unsigned_integer, _integer,
         unsigned_bigint, _bigint, unsigned_float, _float, unsigned_double, _double, _varchar, _char, binary_col,
         _date, _datetime, /* exclude it, because ON UPDATE CURRENT_TIMESTAMP _timestamp, */
         _bool FROM test_database_dml.test_table_1 ORDER BY key FORMAT TSV
        """,
        "1\t2\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\tvarchar\tchar\tbinary\\0\\0\t2020-01-01\t"
        "2020-01-01 00:00:00\t1\n2\t1\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\t"
        "varchar\tchar\tbinary\\0\\0\t2020-01-01\t2020-01-01 00:00:00\t0\n")

    # update primary key
    mysql_node.query("UPDATE test_database_dml.test_table_1 SET `key` = 3 WHERE `unsigned_tiny_int` = 2")

    check_query(clickhouse_node, "SELECT key, unsigned_tiny_int, tiny_int, unsigned_small_int,"
                                 " small_int, unsigned_medium_int, medium_int, unsigned_int, _int, unsigned_integer, _integer, "
                                 " unsigned_bigint, _bigint, unsigned_float, _float, unsigned_double, _double, _varchar, _char, binary_col, "
                                 " _date, _datetime, /* exclude it, because ON UPDATE CURRENT_TIMESTAMP _timestamp, */ "
                                 " _bool FROM test_database_dml.test_table_1 ORDER BY key FORMAT TSV",
                "2\t1\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\t"
                "varchar\tchar\tbinary\\0\\0\t2020-01-01\t2020-01-01 00:00:00\t0\n3\t2\t-1\t2\t-2\t3\t-3\t"
                "4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\tvarchar\tchar\tbinary\\0\\0\t2020-01-01\t2020-01-01 00:00:00\t1\n")

    mysql_node.query('DELETE FROM test_database_dml.test_table_1 WHERE `key` = 2')
    check_query(clickhouse_node, "SELECT key, unsigned_tiny_int, tiny_int, unsigned_small_int,"
                                 " small_int, unsigned_medium_int, medium_int, unsigned_int, _int, unsigned_integer, _integer, "
                                 " unsigned_bigint, _bigint, unsigned_float, _float, unsigned_double, _double, _varchar, _char, binary_col, "
                                 " _date, _datetime, /* exclude it, because ON UPDATE CURRENT_TIMESTAMP _timestamp, */ "
                                 " _bool FROM test_database_dml.test_table_1 ORDER BY key FORMAT TSV",
                "3\t2\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\tvarchar\tchar\tbinary\\0\\0\t2020-01-01\t"
                "2020-01-01 00:00:00\t1\n")

    mysql_node.query('DELETE FROM test_database_dml.test_table_1 WHERE `unsigned_tiny_int` = 2')
    check_query(clickhouse_node, "SELECT * FROM test_database_dml.test_table_1 ORDER BY key FORMAT TSV", "")

    clickhouse_node.query("DROP DATABASE test_database_dml")
    mysql_node.query("DROP DATABASE test_database_dml")


def materialize_mysql_database_with_views(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
    # existed before the mapping was created

    mysql_node.query("CREATE TABLE test_database.test_table_1 ("
                     "`key` INT NOT NULL PRIMARY KEY, "
                     "unsigned_tiny_int TINYINT UNSIGNED, tiny_int TINYINT, "
                     "unsigned_small_int SMALLINT UNSIGNED, small_int SMALLINT, "
                     "unsigned_medium_int MEDIUMINT UNSIGNED, medium_int MEDIUMINT, "
                     "unsigned_int INT UNSIGNED, _int INT, "
                     "unsigned_integer INTEGER UNSIGNED, _integer INTEGER, "
                     "unsigned_bigint BIGINT UNSIGNED, _bigint BIGINT, "
                     "/* Need ClickHouse support read mysql decimal unsigned_decimal DECIMAL(19, 10) UNSIGNED, _decimal DECIMAL(19, 10), */"
                     "unsigned_float FLOAT UNSIGNED, _float FLOAT, "
                     "unsigned_double DOUBLE UNSIGNED, _double DOUBLE, "
                     "_varchar VARCHAR(10), _char CHAR(10), binary_col BINARY(8), "
                     "/* Need ClickHouse support Enum('a', 'b', 'v') _enum ENUM('a', 'b', 'c'), */"
                     "_date Date, _datetime DateTime, _timestamp TIMESTAMP, _bool BOOLEAN) ENGINE = InnoDB;")

    mysql_node.query("CREATE VIEW test_database.test_table_1_view AS SELECT SUM(tiny_int) FROM test_database.test_table_1 GROUP BY _date;")

    # it already has some data
    mysql_node.query("""
        INSERT INTO test_database.test_table_1 VALUES(1, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 3.2, -3.2, 3.4, -3.4, 'varchar', 'char', 'binary',
        '2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00', true);
        """)
    clickhouse_node.query(
        "CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(
            service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\n")

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")


def materialize_mysql_database_with_datetime_and_decimal(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_dt")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_dt")
    mysql_node.query("CREATE DATABASE test_database_dt DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database_dt.test_table_1 (`key` INT NOT NULL PRIMARY KEY, _datetime DateTime(6), _timestamp TIMESTAMP(3), _decimal DECIMAL(65, 30)) ENGINE = InnoDB;")
    mysql_node.query("INSERT INTO test_database_dt.test_table_1 VALUES(1, '2020-01-01 01:02:03.999999', '2020-01-01 01:02:03.999', " + ('9' * 35) + "." + ('9' * 30) + ")")
    mysql_node.query("INSERT INTO test_database_dt.test_table_1 VALUES(2, '2020-01-01 01:02:03.000000', '2020-01-01 01:02:03.000', ." + ('0' * 29) + "1)")
    mysql_node.query("INSERT INTO test_database_dt.test_table_1 VALUES(3, '2020-01-01 01:02:03.9999', '2020-01-01 01:02:03.99', -" + ('9' * 35) + "." + ('9' * 30) + ")")
    mysql_node.query("INSERT INTO test_database_dt.test_table_1 VALUES(4, '2020-01-01 01:02:03.9999', '2020-01-01 01:02:03.9999', -." + ('0' * 29) + "1)")

    clickhouse_node.query("CREATE DATABASE test_database_dt ENGINE = MaterializeMySQL('{}:3306', 'test_database_dt', 'root', 'clickhouse')".format(service_name))
    assert "test_database_dt" in clickhouse_node.query("SHOW DATABASES")

    check_query(clickhouse_node, "SELECT * FROM test_database_dt.test_table_1 ORDER BY key FORMAT TSV",
                "1\t2020-01-01 01:02:03.999999\t2020-01-01 01:02:03.999\t" + ('9' * 35) + "." + ('9' * 30) + "\n"
                "2\t2020-01-01 01:02:03.000000\t2020-01-01 01:02:03.000\t0." + ('0' * 29) + "1\n"
                "3\t2020-01-01 01:02:03.999900\t2020-01-01 01:02:03.990\t-" + ('9' * 35) + "." + ('9' * 30) + "\n"
                "4\t2020-01-01 01:02:03.999900\t2020-01-01 01:02:04.000\t-0." + ('0' * 29) + "1\n")

    mysql_node.query("CREATE TABLE test_database_dt.test_table_2 (`key` INT NOT NULL PRIMARY KEY, _datetime DateTime(6), _timestamp TIMESTAMP(3), _decimal DECIMAL(65, 30)) ENGINE = InnoDB;")
    mysql_node.query("INSERT INTO test_database_dt.test_table_2 VALUES(1, '2020-01-01 01:02:03.999999', '2020-01-01 01:02:03.999', " + ('9' * 35) + "." + ('9' * 30) + ")")
    mysql_node.query("INSERT INTO test_database_dt.test_table_2 VALUES(2, '2020-01-01 01:02:03.000000', '2020-01-01 01:02:03.000', ." + ('0' * 29) + "1)")
    mysql_node.query("INSERT INTO test_database_dt.test_table_2 VALUES(3, '2020-01-01 01:02:03.9999', '2020-01-01 01:02:03.99', -" + ('9' * 35) + "." + ('9' * 30) + ")")
    mysql_node.query("INSERT INTO test_database_dt.test_table_2 VALUES(4, '2020-01-01 01:02:03.9999', '2020-01-01 01:02:03.9999', -." + ('0' * 29) + "1)")

    check_query(clickhouse_node, "SELECT * FROM test_database_dt.test_table_2 ORDER BY key FORMAT TSV",
                "1\t2020-01-01 01:02:03.999999\t2020-01-01 01:02:03.999\t" + ('9' * 35) + "." + ('9' * 30) + "\n"
                "2\t2020-01-01 01:02:03.000000\t2020-01-01 01:02:03.000\t0." + ('0' * 29) + "1\n"
                "3\t2020-01-01 01:02:03.999900\t2020-01-01 01:02:03.990\t-" + ('9' * 35) + "." + ('9' * 30) + "\n"
                "4\t2020-01-01 01:02:03.999900\t2020-01-01 01:02:04.000\t-0." + ('0' * 29) + "1\n")
    clickhouse_node.query("DROP DATABASE test_database_dt")
    mysql_node.query("DROP DATABASE test_database_dt")


def drop_table_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_drop")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_drop")
    mysql_node.query("CREATE DATABASE test_database_drop DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database_drop.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

    mysql_node.query("DROP TABLE test_database_drop.test_table_1;")

    mysql_node.query("CREATE TABLE test_database_drop.test_table_2 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

    mysql_node.query("TRUNCATE TABLE test_database_drop.test_table_2;")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database_drop ENGINE = MaterializeMySQL('{}:3306', 'test_database_drop', 'root', 'clickhouse')".format(
            service_name))

    assert "test_database_drop" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SELECT * FROM test_database_drop.test_table_2 ORDER BY id FORMAT TSV", "")

    mysql_node.query("INSERT INTO test_database_drop.test_table_2 VALUES(1), (2), (3), (4), (5), (6)")
    mysql_node.query("CREATE TABLE test_database_drop.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_drop FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "SELECT * FROM test_database_drop.test_table_2 ORDER BY id FORMAT TSV",
                "1\n2\n3\n4\n5\n6\n")

    mysql_node.query("DROP TABLE test_database_drop.test_table_1;")
    mysql_node.query("TRUNCATE TABLE test_database_drop.test_table_2;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_drop FORMAT TSV", "test_table_2\n")
    check_query(clickhouse_node, "SELECT * FROM test_database_drop.test_table_2 ORDER BY id FORMAT TSV", "")

    clickhouse_node.query("DROP DATABASE test_database_drop")
    mysql_node.query("DROP DATABASE test_database_drop")


def create_table_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_create")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_create")
    mysql_node.query("CREATE DATABASE test_database_create DEFAULT CHARACTER SET 'utf8'")
    # existed before the mapping was created
    mysql_node.query("CREATE TABLE test_database_create.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
    # it already has some data
    mysql_node.query("INSERT INTO test_database_create.test_table_1 VALUES(1), (2), (3), (5), (6), (7);")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database_create ENGINE = MaterializeMySQL('{}:3306', 'test_database_create', 'root', 'clickhouse')".format(
            service_name))

    # Check for pre-existing status
    assert "test_database_create" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SELECT * FROM test_database_create.test_table_1 ORDER BY id FORMAT TSV",
                "1\n2\n3\n5\n6\n7\n")

    mysql_node.query("CREATE TABLE test_database_create.test_table_2 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
    mysql_node.query("INSERT INTO test_database_create.test_table_2 VALUES(1), (2), (3), (4), (5), (6);")
    check_query(clickhouse_node, "SELECT * FROM test_database_create.test_table_2 ORDER BY id FORMAT TSV",
                "1\n2\n3\n4\n5\n6\n")

    clickhouse_node.query("DROP DATABASE test_database_create")
    mysql_node.query("DROP DATABASE test_database_create")


def rename_table_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_rename")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_rename")
    mysql_node.query("CREATE DATABASE test_database_rename DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database_rename.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

    mysql_node.query("RENAME TABLE test_database_rename.test_table_1 TO test_database_rename.test_table_2")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database_rename ENGINE = MaterializeMySQL('{}:3306', 'test_database_rename', 'root', 'clickhouse')".format(
            service_name))

    assert "test_database_rename" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_rename FORMAT TSV", "test_table_2\n")
    mysql_node.query("RENAME TABLE test_database_rename.test_table_2 TO test_database_rename.test_table_1")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_rename FORMAT TSV", "test_table_1\n")

    clickhouse_node.query("DROP DATABASE test_database_rename")
    mysql_node.query("DROP DATABASE test_database_rename")


def alter_add_column_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_add")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_add")
    mysql_node.query("CREATE DATABASE test_database_add DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database_add.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database_add.test_table_1 ADD COLUMN add_column_1 INT NOT NULL")
    mysql_node.query("ALTER TABLE test_database_add.test_table_1 ADD COLUMN add_column_2 INT NOT NULL FIRST")
    mysql_node.query("ALTER TABLE test_database_add.test_table_1 ADD COLUMN add_column_3 INT NOT NULL AFTER add_column_1")
    mysql_node.query("ALTER TABLE test_database_add.test_table_1 ADD COLUMN add_column_4 INT NOT NULL DEFAULT " + (
        "0" if service_name == "mysql57" else "(id)"))

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database_add ENGINE = MaterializeMySQL('{}:3306', 'test_database_add', 'root', 'clickhouse')".format(
            service_name))

    assert "test_database_add" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "DESC test_database_add.test_table_1 FORMAT TSV",
                "add_column_2\tInt32\t\t\t\t\t\nid\tInt32\t\t\t\t\t\nadd_column_1\tInt32\t\t\t\t\t\nadd_column_3\tInt32\t\t\t\t\t\nadd_column_4\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("CREATE TABLE test_database_add.test_table_2 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_add FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database_add.test_table_2 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query(
        "ALTER TABLE test_database_add.test_table_2 ADD COLUMN add_column_1 INT NOT NULL, ADD COLUMN add_column_2 INT NOT NULL FIRST")
    mysql_node.query(
        "ALTER TABLE test_database_add.test_table_2 ADD COLUMN add_column_3 INT NOT NULL AFTER add_column_1, ADD COLUMN add_column_4 INT NOT NULL DEFAULT " + (
            "0" if service_name == "mysql57" else "(id)"))

    default_expression = "DEFAULT\t0" if service_name == "mysql57" else "DEFAULT\tid"
    check_query(clickhouse_node, "DESC test_database_add.test_table_2 FORMAT TSV",
                "add_column_2\tInt32\t\t\t\t\t\nid\tInt32\t\t\t\t\t\nadd_column_1\tInt32\t\t\t\t\t\nadd_column_3\tInt32\t\t\t\t\t\nadd_column_4\tInt32\t" + default_expression + "\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    mysql_node.query("INSERT INTO test_database_add.test_table_2 VALUES(1, 2, 3, 4, 5), (6, 7, 8, 9, 10)")
    check_query(clickhouse_node, "SELECT * FROM test_database_add.test_table_2 ORDER BY id FORMAT TSV",
                "1\t2\t3\t4\t5\n6\t7\t8\t9\t10\n")

    clickhouse_node.query("DROP DATABASE test_database_add")
    mysql_node.query("DROP DATABASE test_database_add")


def alter_drop_column_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_alter_drop")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_alter_drop")
    mysql_node.query("CREATE DATABASE test_database_alter_drop DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query(
        "CREATE TABLE test_database_alter_drop.test_table_1 (id INT NOT NULL PRIMARY KEY, drop_column INT) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database_alter_drop.test_table_1 DROP COLUMN drop_column")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database_alter_drop ENGINE = MaterializeMySQL('{}:3306', 'test_database_alter_drop', 'root', 'clickhouse')".format(
            service_name))

    assert "test_database_alter_drop" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_alter_drop FORMAT TSV", "test_table_1\n")
    check_query(clickhouse_node, "DESC test_database_alter_drop.test_table_1 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query(
        "CREATE TABLE test_database_alter_drop.test_table_2 (id INT NOT NULL PRIMARY KEY, drop_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_alter_drop FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database_alter_drop.test_table_2 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\ndrop_column\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database_alter_drop.test_table_2 DROP COLUMN drop_column")
    check_query(clickhouse_node, "DESC test_database_alter_drop.test_table_2 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    mysql_node.query("INSERT INTO test_database_alter_drop.test_table_2 VALUES(1), (2), (3), (4), (5)")
    check_query(clickhouse_node, "SELECT * FROM test_database_alter_drop.test_table_2 ORDER BY id FORMAT TSV", "1\n2\n3\n4\n5\n")

    clickhouse_node.query("DROP DATABASE test_database_alter_drop")
    mysql_node.query("DROP DATABASE test_database_alter_drop")


def alter_rename_column_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_alter_rename")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_alter_rename")
    mysql_node.query("CREATE DATABASE test_database_alter_rename DEFAULT CHARACTER SET 'utf8'")

    # maybe should test rename primary key?
    mysql_node.query(
        "CREATE TABLE test_database_alter_rename.test_table_1 (id INT NOT NULL PRIMARY KEY, rename_column INT NOT NULL) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database_alter_rename.test_table_1 RENAME COLUMN rename_column TO new_column_name")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database_alter_rename ENGINE = MaterializeMySQL('{}:3306', 'test_database_alter_rename', 'root', 'clickhouse')".format(
            service_name))

    assert "test_database_alter_rename" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "DESC test_database_alter_rename.test_table_1 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\nnew_column_name\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query(
        "CREATE TABLE test_database_alter_rename.test_table_2 (id INT NOT NULL PRIMARY KEY, rename_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "DESC test_database_alter_rename.test_table_2 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\nrename_column\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database_alter_rename.test_table_2 RENAME COLUMN rename_column TO new_column_name")
    check_query(clickhouse_node, "DESC test_database_alter_rename.test_table_2 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\nnew_column_name\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    mysql_node.query("INSERT INTO test_database_alter_rename.test_table_2 VALUES(1, 2), (3, 4), (5, 6), (7, 8), (9, 10)")
    check_query(clickhouse_node, "SELECT * FROM test_database_alter_rename.test_table_2 ORDER BY id FORMAT TSV",
                "1\t2\n3\t4\n5\t6\n7\t8\n9\t10\n")

    clickhouse_node.query("DROP DATABASE test_database_alter_rename")
    mysql_node.query("DROP DATABASE test_database_alter_rename")


def alter_modify_column_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_alter_modify")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_alter_modify")
    mysql_node.query("CREATE DATABASE test_database_alter_modify DEFAULT CHARACTER SET 'utf8'")

    # maybe should test rename primary key?
    mysql_node.query(
        "CREATE TABLE test_database_alter_modify.test_table_1 (id INT NOT NULL PRIMARY KEY, modify_column INT NOT NULL) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database_alter_modify.test_table_1 MODIFY COLUMN modify_column INT")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database_alter_modify ENGINE = MaterializeMySQL('{}:3306', 'test_database_alter_modify', 'root', 'clickhouse')".format(
            service_name))

    assert "test_database_alter_modify" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_alter_modify FORMAT TSV", "test_table_1\n")
    check_query(clickhouse_node, "DESC test_database_alter_modify.test_table_1 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\nmodify_column\tNullable(Int32)\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query(
        "CREATE TABLE test_database_alter_modify.test_table_2 (id INT NOT NULL PRIMARY KEY, modify_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_alter_modify FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database_alter_modify.test_table_2 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\nmodify_column\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database_alter_modify.test_table_2 MODIFY COLUMN modify_column INT")
    check_query(clickhouse_node, "DESC test_database_alter_modify.test_table_2 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\nmodify_column\tNullable(Int32)\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database_alter_modify.test_table_2 MODIFY COLUMN modify_column INT FIRST")
    check_query(clickhouse_node, "DESC test_database_alter_modify.test_table_2 FORMAT TSV",
                "modify_column\tNullable(Int32)\t\t\t\t\t\nid\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database_alter_modify.test_table_2 MODIFY COLUMN modify_column INT AFTER id")
    check_query(clickhouse_node, "DESC test_database_alter_modify.test_table_2 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\nmodify_column\tNullable(Int32)\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    mysql_node.query("INSERT INTO test_database_alter_modify.test_table_2 VALUES(1, 2), (3, NULL)")
    check_query(clickhouse_node, "SELECT * FROM test_database_alter_modify.test_table_2 ORDER BY id FORMAT TSV", "1\t2\n3\t\\N\n")

    clickhouse_node.query("DROP DATABASE test_database_alter_modify")
    mysql_node.query("DROP DATABASE test_database_alter_modify")


# TODO: need ClickHouse support ALTER TABLE table_name ADD COLUMN column_name, RENAME COLUMN column_name TO new_column_name;
# def test_mysql_alter_change_column_for_materialize_mysql_database(started_cluster):
#     pass

def alter_rename_table_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_rename_table")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_rename_table")
    mysql_node.query("CREATE DATABASE test_database_rename_table DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query(
        "CREATE TABLE test_database_rename_table.test_table_1 (id INT NOT NULL PRIMARY KEY, drop_column INT) ENGINE = InnoDB;")

    mysql_node.query(
        "ALTER TABLE test_database_rename_table.test_table_1 DROP COLUMN drop_column, RENAME TO test_database_rename_table.test_table_2, RENAME TO test_database_rename_table.test_table_3")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database_rename_table ENGINE = MaterializeMySQL('{}:3306', 'test_database_rename_table', 'root', 'clickhouse')".format(
            service_name))

    assert "test_database_rename_table" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_rename_table FORMAT TSV", "test_table_3\n")
    check_query(clickhouse_node, "DESC test_database_rename_table.test_table_3 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query(
        "CREATE TABLE test_database_rename_table.test_table_1 (id INT NOT NULL PRIMARY KEY, drop_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_rename_table FORMAT TSV", "test_table_1\ntest_table_3\n")
    check_query(clickhouse_node, "DESC test_database_rename_table.test_table_1 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\ndrop_column\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query(
        "ALTER TABLE test_database_rename_table.test_table_1 DROP COLUMN drop_column, RENAME TO test_database_rename_table.test_table_2, RENAME TO test_database_rename_table.test_table_4")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database_rename_table FORMAT TSV", "test_table_3\ntest_table_4\n")
    check_query(clickhouse_node, "DESC test_database_rename_table.test_table_4 FORMAT TSV",
                "id\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    mysql_node.query("INSERT INTO test_database_rename_table.test_table_4 VALUES(1), (2), (3), (4), (5)")
    check_query(clickhouse_node, "SELECT * FROM test_database_rename_table.test_table_4 ORDER BY id FORMAT TSV", "1\n2\n3\n4\n5\n")

    clickhouse_node.query("DROP DATABASE test_database_rename_table")
    mysql_node.query("DROP DATABASE test_database_rename_table")


def query_event_with_empty_transaction(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database_event")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_event")
    mysql_node.query("CREATE DATABASE test_database_event")

    mysql_node.query("RESET MASTER")
    mysql_node.query("CREATE TABLE test_database_event.t1(a INT NOT NULL PRIMARY KEY, b VARCHAR(255) DEFAULT 'BEGIN')")
    mysql_node.query("INSERT INTO test_database_event.t1(a) VALUES(1)")

    clickhouse_node.query(
        "CREATE DATABASE test_database_event ENGINE = MaterializeMySQL('{}:3306', 'test_database_event', 'root', 'clickhouse')".format(
            service_name))

    # Reject one empty GTID QUERY event with 'BEGIN' and 'COMMIT'
    mysql_cursor = mysql_node.alloc_connection().cursor(pymysql.cursors.DictCursor)
    mysql_cursor.execute("SHOW MASTER STATUS")
    (uuid, seqs) = mysql_cursor.fetchall()[0]["Executed_Gtid_Set"].split(":")
    (seq_begin, seq_end) = seqs.split("-")
    next_gtid = uuid + ":" + str(int(seq_end) + 1)
    mysql_node.query("SET gtid_next='" + next_gtid + "'")
    mysql_node.query("BEGIN")
    mysql_node.query("COMMIT")
    mysql_node.query("SET gtid_next='AUTOMATIC'")

    # Reject one 'BEGIN' QUERY event and 'COMMIT' XID event.
    mysql_node.query("/* start */ begin /* end */")
    mysql_node.query("INSERT INTO test_database_event.t1(a) VALUES(2)")
    mysql_node.query("/* start */ commit /* end */")

    check_query(clickhouse_node, "SHOW TABLES FROM test_database_event FORMAT TSV", "t1\n")
    check_query(clickhouse_node, "SELECT * FROM test_database_event.t1 ORDER BY a FORMAT TSV", "1\tBEGIN\n2\tBEGIN\n")
    clickhouse_node.query("DROP DATABASE test_database_event")
    mysql_node.query("DROP DATABASE test_database_event")


def select_without_columns(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS db")
    clickhouse_node.query("DROP DATABASE IF EXISTS db")
    mysql_node.query("CREATE DATABASE db")
    mysql_node.query("CREATE TABLE db.t (a INT PRIMARY KEY, b INT)")
    clickhouse_node.query(
        "CREATE DATABASE db ENGINE = MaterializeMySQL('{}:3306', 'db', 'root', 'clickhouse') SETTINGS max_flush_data_time = 100000".format(service_name))
    check_query(clickhouse_node, "SHOW TABLES FROM db FORMAT TSV", "t\n")
    clickhouse_node.query("SYSTEM STOP MERGES db.t")
    clickhouse_node.query("CREATE VIEW v AS SELECT * FROM db.t")
    mysql_node.query("INSERT INTO db.t VALUES (1, 1), (2, 2)")
    mysql_node.query("DELETE FROM db.t WHERE a = 2;")
    # We need to execute a DDL for flush data buffer
    mysql_node.query("CREATE TABLE db.temporary(a INT PRIMARY KEY, b INT)")

    optimize_on_insert = clickhouse_node.query("SELECT value FROM system.settings WHERE name='optimize_on_insert'").strip()
    if optimize_on_insert == "0":
        res = ["3\n", "2\n", "2\n"]
    else:
        res = ["2\n", "2\n", "1\n"]
    check_query(clickhouse_node, "SELECT count((_sign, _version)) FROM db.t FORMAT TSV", res[0])

    assert clickhouse_node.query("SELECT count(_sign) FROM db.t FORMAT TSV") == res[1]
    assert_eq_with_retry(clickhouse_node, "SELECT count(_version) FROM db.t", res[2].strip(), sleep_time=2, retry_count=3)

    assert clickhouse_node.query("SELECT count() FROM db.t FORMAT TSV") == "1\n"
    assert clickhouse_node.query("SELECT count(*) FROM db.t FORMAT TSV") == "1\n"
    assert clickhouse_node.query("SELECT count() FROM (SELECT * FROM db.t) FORMAT TSV") == "1\n"
    assert clickhouse_node.query("SELECT count() FROM v FORMAT TSV") == "1\n"
    assert clickhouse_node.query("SELECT count() FROM merge('db', 't') FORMAT TSV") == "1\n"
    assert clickhouse_node.query("SELECT count() FROM remote('localhost', 'db', 't') FORMAT TSV") == "1\n"

    assert clickhouse_node.query("SELECT _part FROM db.t FORMAT TSV") == "0_1_1_0\n"
    assert clickhouse_node.query("SELECT _part FROM remote('localhost', 'db', 't') FORMAT TSV") == "0_1_1_0\n"


    clickhouse_node.query("DROP VIEW v")
    clickhouse_node.query("DROP DATABASE db")
    mysql_node.query("DROP DATABASE db")


def insert_with_modify_binlog_checksum(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_checksum")
    mysql_node.query("CREATE TABLE test_checksum.t (a INT PRIMARY KEY, b varchar(200))")
    clickhouse_node.query("CREATE DATABASE test_checksum ENGINE = MaterializeMySQL('{}:3306', 'test_checksum', 'root', 'clickhouse')".format(service_name))
    check_query(clickhouse_node, "SHOW TABLES FROM test_checksum FORMAT TSV", "t\n")
    mysql_node.query("INSERT INTO test_checksum.t VALUES(1, '1111')")
    check_query(clickhouse_node, "SELECT * FROM test_checksum.t ORDER BY a FORMAT TSV", "1\t1111\n")

    mysql_node.query("SET GLOBAL binlog_checksum=NONE")
    mysql_node.query("INSERT INTO test_checksum.t VALUES(2, '2222')")
    check_query(clickhouse_node, "SELECT * FROM test_checksum.t ORDER BY a FORMAT TSV", "1\t1111\n2\t2222\n")

    mysql_node.query("SET GLOBAL binlog_checksum=CRC32")
    mysql_node.query("INSERT INTO test_checksum.t VALUES(3, '3333')")
    check_query(clickhouse_node, "SELECT * FROM test_checksum.t ORDER BY a FORMAT TSV", "1\t1111\n2\t2222\n3\t3333\n")

    clickhouse_node.query("DROP DATABASE test_checksum")
    mysql_node.query("DROP DATABASE test_checksum")


def err_sync_user_privs_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    clickhouse_node.query("DROP DATABASE IF EXISTS priv_err_db")
    mysql_node.query("DROP DATABASE IF EXISTS priv_err_db")
    mysql_node.query("CREATE DATABASE priv_err_db DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE priv_err_db.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
    mysql_node.query("INSERT INTO priv_err_db.test_table_1 VALUES(1);")
    mysql_node.create_min_priv_user("test", "123")
    mysql_node.result("SHOW GRANTS FOR 'test'@'%';")

    clickhouse_node.query(
        "CREATE DATABASE priv_err_db ENGINE = MaterializeMySQL('{}:3306', 'priv_err_db', 'test', '123')".format(
            service_name))

    check_query(clickhouse_node, "SELECT count() FROM priv_err_db.test_table_1 FORMAT TSV", "1\n", 30, 5)
    mysql_node.query("INSERT INTO priv_err_db.test_table_1 VALUES(2);")
    check_query(clickhouse_node, "SELECT count() FROM priv_err_db.test_table_1 FORMAT TSV", "2\n")
    clickhouse_node.query("DROP DATABASE priv_err_db;")

    mysql_node.query("REVOKE REPLICATION SLAVE ON *.* FROM 'test'@'%'")
    clickhouse_node.query(
        "CREATE DATABASE priv_err_db ENGINE = MaterializeMySQL('{}:3306', 'priv_err_db', 'test', '123')".format(
            service_name))
    assert "priv_err_db" in clickhouse_node.query("SHOW DATABASES")
    assert "test_table_1" not in clickhouse_node.query("SHOW TABLES FROM priv_err_db")
    clickhouse_node.query("DROP DATABASE priv_err_db")

    mysql_node.query("REVOKE REPLICATION CLIENT, RELOAD ON *.* FROM 'test'@'%'")
    clickhouse_node.query(
        "CREATE DATABASE priv_err_db ENGINE = MaterializeMySQL('{}:3306', 'priv_err_db', 'test', '123')".format(
            service_name))
    assert "priv_err_db" in clickhouse_node.query("SHOW DATABASES")
    assert "test_table_1" not in clickhouse_node.query("SHOW TABLES FROM priv_err_db")
    clickhouse_node.query("DETACH DATABASE priv_err_db")

    mysql_node.query("REVOKE SELECT ON priv_err_db.* FROM 'test'@'%'")
    time.sleep(3)

    with pytest.raises(QueryRuntimeException) as exception:
        clickhouse_node.query("ATTACH DATABASE priv_err_db")

    assert 'MySQL SYNC USER ACCESS ERR:' in str(exception.value)
    assert "priv_err_db" not in clickhouse_node.query("SHOW DATABASES")

    mysql_node.query("GRANT SELECT ON priv_err_db.* TO 'test'@'%'")
    time.sleep(3)
    clickhouse_node.query("ATTACH DATABASE priv_err_db")
    clickhouse_node.query("DROP DATABASE priv_err_db")
    mysql_node.query("REVOKE SELECT ON priv_err_db.* FROM 'test'@'%'")

    mysql_node.query("DROP DATABASE priv_err_db;")
    mysql_node.query("DROP USER 'test'@'%'")


def restore_instance_mysql_connections(clickhouse_node, pm, action='REJECT'):
    pm._check_instance(clickhouse_node)
    pm._delete_rule({'source': clickhouse_node.ip_address, 'destination_port': 3306, 'action': action})
    pm._delete_rule({'destination': clickhouse_node.ip_address, 'source_port': 3306, 'action': action})
    time.sleep(5)

def drop_instance_mysql_connections(clickhouse_node, pm, action='REJECT'):
    pm._check_instance(clickhouse_node)
    pm._add_rule({'source': clickhouse_node.ip_address, 'destination_port': 3306, 'action': action})
    pm._add_rule({'destination': clickhouse_node.ip_address, 'source_port': 3306, 'action': action})
    time.sleep(5)

def network_partition_test(clickhouse_node, mysql_node, service_name):
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_network")
    clickhouse_node.query("DROP DATABASE  IF EXISTS test")
    mysql_node.query("DROP DATABASE IF EXISTS test_database_network")
    mysql_node.query("DROP DATABASE IF EXISTS test")
    mysql_node.query("CREATE DATABASE test_database_network;")
    mysql_node.query("CREATE TABLE test_database_network.test_table ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")

    mysql_node.query("CREATE DATABASE test;")

    clickhouse_node.query(
        "CREATE DATABASE test_database_network ENGINE = MaterializeMySQL('{}:3306', 'test_database_network', 'root', 'clickhouse')".format(service_name))
    check_query(clickhouse_node, "SELECT * FROM test_database_network.test_table", '')

    with PartitionManager() as pm:
        drop_instance_mysql_connections(clickhouse_node, pm)
        mysql_node.query('INSERT INTO test_database_network.test_table VALUES(1)')
        check_query(clickhouse_node, "SELECT * FROM test_database_network.test_table", '')

        with pytest.raises(QueryRuntimeException) as exception:
            clickhouse_node.query(
                "CREATE DATABASE test ENGINE = MaterializeMySQL('{}:3306', 'test', 'root', 'clickhouse')".format(service_name))

        assert "Can't connect to MySQL server" in str(exception.value)

        restore_instance_mysql_connections(clickhouse_node, pm)

        check_query(clickhouse_node, "SELECT * FROM test_database_network.test_table FORMAT TSV", '1\n')

        clickhouse_node.query(
            "CREATE DATABASE test ENGINE = MaterializeMySQL('{}:3306', 'test', 'root', 'clickhouse')".format(service_name))
        check_query(clickhouse_node, "SHOW TABLES FROM test_database_network FORMAT TSV", "test_table\n")

        mysql_node.query("CREATE TABLE test.test ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
        check_query(clickhouse_node, "SHOW TABLES FROM test FORMAT TSV", "test\n")

        clickhouse_node.query("DROP DATABASE test_database_network")
        clickhouse_node.query("DROP DATABASE test")
        mysql_node.query("DROP DATABASE test_database_network")
        mysql_node.query("DROP DATABASE test")


def mysql_kill_sync_thread_restore_test(clickhouse_node, mysql_node, service_name):
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database;")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database_auto;")

    mysql_node.query("DROP DATABASE IF EXISTS test_database;")
    mysql_node.query("CREATE DATABASE test_database;")
    mysql_node.query("CREATE TABLE test_database.test_table ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
    mysql_node.query("INSERT INTO test_database.test_table VALUES (1)")

    mysql_node.query("DROP DATABASE IF EXISTS test_database_auto;")
    mysql_node.query("CREATE DATABASE test_database_auto;")
    mysql_node.query("CREATE TABLE test_database_auto.test_table ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
    mysql_node.query("INSERT INTO test_database_auto.test_table VALUES (11)")

    clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse') SETTINGS max_wait_time_when_mysql_unavailable=-1".format(service_name))
    clickhouse_node.query("CREATE DATABASE test_database_auto ENGINE = MaterializeMySQL('{}:3306', 'test_database_auto', 'root', 'clickhouse')".format(service_name))

    check_query(clickhouse_node, "SELECT * FROM test_database.test_table FORMAT TSV", '1\n')
    check_query(clickhouse_node, "SELECT * FROM test_database_auto.test_table FORMAT TSV", '11\n')


    # When ClickHouse dump all history data we can query it on ClickHouse
    # but it don't mean that the sync thread is already to connect to MySQL.
    # So After ClickHouse can query data, insert some rows to MySQL. Use this to re-check sync successed.
    mysql_node.query("INSERT INTO test_database_auto.test_table VALUES (22)")
    mysql_node.query("INSERT INTO test_database.test_table VALUES (2)")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table ORDER BY id FORMAT TSV", '1\n2\n')
    check_query(clickhouse_node, "SELECT * FROM test_database_auto.test_table ORDER BY id FORMAT TSV", '11\n22\n')

    get_sync_id_query = "SELECT id FROM information_schema.processlist WHERE state LIKE '% has sent all binlog to % waiting for more updates%';"
    result = mysql_node.query_and_get_data(get_sync_id_query)
    assert len(result) > 0
    for row in result:
        query = "kill " + str(row[0]) + ";"
        mysql_node.query(query)

    with pytest.raises(QueryRuntimeException, match="Cannot read all data"):
        # https://dev.mysql.com/doc/refman/5.7/en/kill.html
        # When you use KILL, a thread-specific kill flag is set for the thread.
        # In most cases, it might take some time for the thread to die because the kill flag is checked only at specific intervals.
        for sleep_time in [1, 3, 5]:
            time.sleep(sleep_time)
            clickhouse_node.query("SELECT * FROM test_database.test_table")

    clickhouse_node.query("DETACH DATABASE test_database")
    clickhouse_node.query("ATTACH DATABASE test_database")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table ORDER BY id FORMAT TSV", '1\n2\n')

    mysql_node.query("INSERT INTO test_database.test_table VALUES (3)")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table ORDER BY id FORMAT TSV", '1\n2\n3\n')

    mysql_node.query("INSERT INTO test_database_auto.test_table VALUES (33)")
    check_query(clickhouse_node, "SELECT * FROM test_database_auto.test_table ORDER BY id FORMAT TSV", '11\n22\n33\n')

    clickhouse_node.query("DROP DATABASE test_database")
    clickhouse_node.query("DROP DATABASE test_database_auto")
    mysql_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database_auto")


def mysql_killed_while_insert(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS kill_mysql_while_insert")
    clickhouse_node.query("DROP DATABASE IF EXISTS kill_mysql_while_insert")
    mysql_node.query("CREATE DATABASE kill_mysql_while_insert")
    mysql_node.query("CREATE TABLE kill_mysql_while_insert.test ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
    clickhouse_node.query("CREATE DATABASE kill_mysql_while_insert ENGINE = MaterializeMySQL('{}:3306', 'kill_mysql_while_insert', 'root', 'clickhouse') SETTINGS max_wait_time_when_mysql_unavailable=-1".format(service_name))
    check_query(clickhouse_node, "SHOW TABLES FROM kill_mysql_while_insert FORMAT TSV", 'test\n')

    try:
        def insert(num):
            for i in range(num):
                query = "INSERT INTO kill_mysql_while_insert.test VALUES({v});".format( v = i + 1 )
                mysql_node.query(query)

        t = threading.Thread(target=insert, args=(10000,))
        t.start()

        clickhouse_node.cluster.restart_service(service_name)
    finally:
        with pytest.raises(QueryRuntimeException) as exception:
            time.sleep(2)
            clickhouse_node.query("SELECT count() FROM kill_mysql_while_insert.test")

        mysql_node.alloc_connection()

        clickhouse_node.query("DETACH DATABASE kill_mysql_while_insert")
        clickhouse_node.query("ATTACH DATABASE kill_mysql_while_insert")

        result = mysql_node.query_and_get_data("SELECT COUNT(1) FROM kill_mysql_while_insert.test")
        for row in result:
            res = str(row[0]) + '\n'
            check_query(clickhouse_node, "SELECT count() FROM kill_mysql_while_insert.test", res)

        mysql_node.query("DROP DATABASE kill_mysql_while_insert")
        clickhouse_node.query("DROP DATABASE kill_mysql_while_insert")


def clickhouse_killed_while_insert(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS kill_clickhouse_while_insert")
    mysql_node.query("CREATE DATABASE kill_clickhouse_while_insert")
    mysql_node.query("CREATE TABLE kill_clickhouse_while_insert.test ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
    clickhouse_node.query("CREATE DATABASE kill_clickhouse_while_insert ENGINE = MaterializeMySQL('{}:3306', 'kill_clickhouse_while_insert', 'root', 'clickhouse')".format(service_name))
    check_query(clickhouse_node, "SHOW TABLES FROM kill_clickhouse_while_insert FORMAT TSV", 'test\n')

    def insert(num):
        for i in range(num):
            query = "INSERT INTO kill_clickhouse_while_insert.test VALUES({v});".format( v = i + 1 )
            mysql_node.query(query)

    t = threading.Thread(target=insert, args=(1000,))
    t.start()

    # TODO: add clickhouse_node.restart_clickhouse(20, kill=False) test
    clickhouse_node.restart_clickhouse(20, kill=True)
    t.join()

    result = mysql_node.query_and_get_data("SELECT COUNT(1) FROM kill_clickhouse_while_insert.test")
    for row in result:
        res = str(row[0]) + '\n'
        check_query(clickhouse_node, "SELECT count() FROM kill_clickhouse_while_insert.test FORMAT TSV", res)

    mysql_node.query("DROP DATABASE kill_clickhouse_while_insert")
    clickhouse_node.query("DROP DATABASE kill_clickhouse_while_insert")

def utf8mb4_test(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS utf8mb4_test")
    clickhouse_node.query("DROP DATABASE IF EXISTS utf8mb4_test")
    mysql_node.query("CREATE DATABASE utf8mb4_test")
    mysql_node.query("CREATE TABLE utf8mb4_test.test (id INT(11) NOT NULL PRIMARY KEY, name VARCHAR(255)) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4")
    mysql_node.query("INSERT INTO utf8mb4_test.test VALUES(1, '🦄'),(2, '\u2601')")
    clickhouse_node.query("CREATE DATABASE utf8mb4_test ENGINE = MaterializeMySQL('{}:3306', 'utf8mb4_test', 'root', 'clickhouse')".format(service_name))
    check_query(clickhouse_node, "SHOW TABLES FROM utf8mb4_test FORMAT TSV", "test\n")
    check_query(clickhouse_node, "SELECT id, name FROM utf8mb4_test.test ORDER BY id", "1\t\U0001F984\n2\t\u2601\n")

def system_parts_test(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS system_parts_test")
    clickhouse_node.query("DROP DATABASE IF EXISTS system_parts_test")
    mysql_node.query("CREATE DATABASE system_parts_test")
    mysql_node.query("CREATE TABLE system_parts_test.test ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;")
    mysql_node.query("INSERT INTO system_parts_test.test VALUES(1),(2),(3)")
    def check_active_parts(num):
        check_query(clickhouse_node, "SELECT count() FROM system.parts WHERE database = 'system_parts_test' AND table = 'test' AND active = 1", "{}\n".format(num))
    clickhouse_node.query("CREATE DATABASE system_parts_test ENGINE = MaterializeMySQL('{}:3306', 'system_parts_test', 'root', 'clickhouse')".format(service_name))
    check_active_parts(1)
    mysql_node.query("INSERT INTO system_parts_test.test VALUES(4),(5),(6)")
    check_active_parts(2)
    clickhouse_node.query("OPTIMIZE TABLE system_parts_test.test")
    check_active_parts(1)

def multi_table_update_test(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS multi_table_update")
    clickhouse_node.query("DROP DATABASE IF EXISTS multi_table_update")
    mysql_node.query("CREATE DATABASE multi_table_update")
    mysql_node.query("CREATE TABLE multi_table_update.a (id INT(11) NOT NULL PRIMARY KEY, value VARCHAR(255))")
    mysql_node.query("CREATE TABLE multi_table_update.b (id INT(11) NOT NULL PRIMARY KEY, othervalue VARCHAR(255))")
    mysql_node.query("INSERT INTO multi_table_update.a VALUES(1, 'foo')")
    mysql_node.query("INSERT INTO multi_table_update.b VALUES(1, 'bar')")
    clickhouse_node.query("CREATE DATABASE multi_table_update ENGINE = MaterializeMySQL('{}:3306', 'multi_table_update', 'root', 'clickhouse')".format(service_name))
    check_query(clickhouse_node, "SHOW TABLES FROM multi_table_update", "a\nb\n")
    mysql_node.query("UPDATE multi_table_update.a, multi_table_update.b SET value='baz', othervalue='quux' where a.id=b.id")

    check_query(clickhouse_node, "SELECT * FROM multi_table_update.a", "1\tbaz\n")
    check_query(clickhouse_node, "SELECT * FROM multi_table_update.b", "1\tquux\n")

def system_tables_test(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS system_tables_test")
    clickhouse_node.query("DROP DATABASE IF EXISTS system_tables_test")
    mysql_node.query("CREATE DATABASE system_tables_test")
    mysql_node.query("CREATE TABLE system_tables_test.test (id int NOT NULL PRIMARY KEY) ENGINE=InnoDB")
    clickhouse_node.query("CREATE DATABASE system_tables_test ENGINE = MaterializeMySQL('{}:3306', 'system_tables_test', 'root', 'clickhouse')".format(service_name))
    check_query(clickhouse_node, "SELECT partition_key, sorting_key, primary_key FROM system.tables WHERE database = 'system_tables_test' AND name = 'test'", "intDiv(id, 4294967)\tid\tid\n")

def materialize_with_column_comments_test(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS materialize_with_column_comments_test")
    clickhouse_node.query("DROP DATABASE IF EXISTS materialize_with_column_comments_test")
    mysql_node.query("CREATE DATABASE materialize_with_column_comments_test")
    mysql_node.query("CREATE TABLE materialize_with_column_comments_test.test (id int NOT NULL PRIMARY KEY, value VARCHAR(255) COMMENT 'test comment') ENGINE=InnoDB")
    clickhouse_node.query("CREATE DATABASE materialize_with_column_comments_test ENGINE = MaterializeMySQL('{}:3306', 'materialize_with_column_comments_test', 'root', 'clickhouse')".format(service_name))
    check_query(clickhouse_node, "DESCRIBE TABLE materialize_with_column_comments_test.test", "id\tInt32\t\t\t\t\t\nvalue\tNullable(String)\t\t\ttest comment\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE materialize_with_column_comments_test.test MODIFY value VARCHAR(255) COMMENT 'comment test'")
    check_query(clickhouse_node, "DESCRIBE TABLE materialize_with_column_comments_test.test", "id\tInt32\t\t\t\t\t\nvalue\tNullable(String)\t\t\tcomment test\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE materialize_with_column_comments_test.test ADD value2 int COMMENT 'test comment 2'")
    check_query(clickhouse_node, "DESCRIBE TABLE materialize_with_column_comments_test.test", "id\tInt32\t\t\t\t\t\nvalue\tNullable(String)\t\t\tcomment test\t\t\nvalue2\tNullable(Int32)\t\t\ttest comment 2\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    clickhouse_node.query("DROP DATABASE materialize_with_column_comments_test")
    mysql_node.query("DROP DATABASE materialize_with_column_comments_test")

def materialize_with_enum8_test(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS materialize_with_enum8_test")
    clickhouse_node.query("DROP DATABASE IF EXISTS materialize_with_enum8_test")
    mysql_node.query("CREATE DATABASE materialize_with_enum8_test")
    enum8_values_count = 127
    enum8_values = ""
    enum8_values_with_backslash = ""
    for i in range(1, enum8_values_count):
        enum8_values += '\'' + str(i) + "\', "
        enum8_values_with_backslash += "\\\'" + str(i) +"\\\' = " + str(i) + ", "
    enum8_values += '\'' + str(enum8_values_count) + '\''
    enum8_values_with_backslash += "\\\'" + str(enum8_values_count) +"\\\' = " + str(enum8_values_count)
    mysql_node.query("CREATE TABLE materialize_with_enum8_test.test (id int NOT NULL PRIMARY KEY, value ENUM(" + enum8_values + ")) ENGINE=InnoDB")
    mysql_node.query("INSERT INTO materialize_with_enum8_test.test (id, value) VALUES (1, '1'),(2, '2')")
    clickhouse_node.query("CREATE DATABASE materialize_with_enum8_test ENGINE = MaterializeMySQL('{}:3306', 'materialize_with_enum8_test', 'root', 'clickhouse')".format(service_name))
    check_query(clickhouse_node, "SELECT value FROM materialize_with_enum8_test.test ORDER BY id", "1\n2\n")
    mysql_node.query("INSERT INTO materialize_with_enum8_test.test (id, value) VALUES (3, '127')")
    check_query(clickhouse_node, "SELECT value FROM materialize_with_enum8_test.test ORDER BY id", "1\n2\n127\n")
    check_query(clickhouse_node, "DESCRIBE TABLE materialize_with_enum8_test.test", "id\tInt32\t\t\t\t\t\nvalue\tNullable(Enum8(" + enum8_values_with_backslash + "))\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    clickhouse_node.query("DROP DATABASE materialize_with_enum8_test")
    mysql_node.query("DROP DATABASE materialize_with_enum8_test")

def materialize_with_enum16_test(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS materialize_with_enum16_test")
    clickhouse_node.query("DROP DATABASE IF EXISTS materialize_with_enum16_test")
    mysql_node.query("CREATE DATABASE materialize_with_enum16_test")
    enum16_values_count = 600
    enum16_values = ""
    enum16_values_with_backslash = ""
    for i in range(1, enum16_values_count):
        enum16_values += '\'' + str(i) + "\', "
        enum16_values_with_backslash += "\\\'" + str(i) +"\\\' = " + str(i) + ", "
    enum16_values += '\'' + str(enum16_values_count) + '\''
    enum16_values_with_backslash += "\\\'" + str(enum16_values_count) +"\\\' = " + str(enum16_values_count)
    mysql_node.query("CREATE TABLE materialize_with_enum16_test.test (id int NOT NULL PRIMARY KEY, value ENUM(" + enum16_values + ")) ENGINE=InnoDB")
    mysql_node.query("INSERT INTO materialize_with_enum16_test.test (id, value) VALUES (1, '1'),(2, '2')")
    clickhouse_node.query("CREATE DATABASE materialize_with_enum16_test ENGINE = MaterializeMySQL('{}:3306', 'materialize_with_enum16_test', 'root', 'clickhouse')".format(service_name))
    check_query(clickhouse_node, "SELECT value FROM materialize_with_enum16_test.test ORDER BY id", "1\n2\n")
    mysql_node.query("INSERT INTO materialize_with_enum16_test.test (id, value) VALUES (3, '500')")
    check_query(clickhouse_node, "SELECT value FROM materialize_with_enum16_test.test ORDER BY id", "1\n2\n500\n")
    check_query(clickhouse_node, "DESCRIBE TABLE materialize_with_enum16_test.test", "id\tInt32\t\t\t\t\t\nvalue\tNullable(Enum16(" + enum16_values_with_backslash + "))\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    clickhouse_node.query("DROP DATABASE materialize_with_enum16_test")
    mysql_node.query("DROP DATABASE materialize_with_enum16_test")

def alter_enum8_to_enum16_test(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS alter_enum8_to_enum16_test")
    clickhouse_node.query("DROP DATABASE IF EXISTS alter_enum8_to_enum16_test")
    mysql_node.query("CREATE DATABASE alter_enum8_to_enum16_test")

    enum8_values_count = 100
    enum8_values = ""
    enum8_values_with_backslash = ""
    for i in range(1, enum8_values_count):
        enum8_values += '\'' + str(i) + "\', "
        enum8_values_with_backslash += "\\\'" + str(i) +"\\\' = " + str(i) + ", "
    enum8_values += '\'' + str(enum8_values_count) + '\''
    enum8_values_with_backslash += "\\\'" + str(enum8_values_count) +"\\\' = " + str(enum8_values_count)
    mysql_node.query("CREATE TABLE alter_enum8_to_enum16_test.test (id int NOT NULL PRIMARY KEY, value ENUM(" + enum8_values + ")) ENGINE=InnoDB")
    mysql_node.query("INSERT INTO alter_enum8_to_enum16_test.test (id, value) VALUES (1, '1'),(2, '2')")
    clickhouse_node.query("CREATE DATABASE alter_enum8_to_enum16_test ENGINE = MaterializeMySQL('{}:3306', 'alter_enum8_to_enum16_test', 'root', 'clickhouse')".format(service_name))
    mysql_node.query("INSERT INTO alter_enum8_to_enum16_test.test (id, value) VALUES (3, '75')")
    check_query(clickhouse_node, "SELECT value FROM alter_enum8_to_enum16_test.test ORDER BY id", "1\n2\n75\n")
    check_query(clickhouse_node, "DESCRIBE TABLE alter_enum8_to_enum16_test.test", "id\tInt32\t\t\t\t\t\nvalue\tNullable(Enum8(" + enum8_values_with_backslash + "))\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    enum16_values_count = 600
    enum16_values = ""
    enum16_values_with_backslash = ""
    for i in range(1, enum16_values_count):
        enum16_values += '\'' + str(i) + "\', "
        enum16_values_with_backslash += "\\\'" + str(i) +"\\\' = " + str(i) + ", "
    enum16_values += '\'' + str(enum16_values_count) + '\''
    enum16_values_with_backslash += "\\\'" + str(enum16_values_count) +"\\\' = " + str(enum16_values_count)
    mysql_node.query("ALTER TABLE alter_enum8_to_enum16_test.test MODIFY COLUMN value ENUM(" + enum16_values + ")")
    check_query(clickhouse_node, "DESCRIBE TABLE alter_enum8_to_enum16_test.test", "id\tInt32\t\t\t\t\t\nvalue\tNullable(Enum16(" + enum16_values_with_backslash + "))\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("INSERT INTO alter_enum8_to_enum16_test.test (id, value) VALUES (4, '500')")
    check_query(clickhouse_node, "SELECT value FROM alter_enum8_to_enum16_test.test ORDER BY id", "1\n2\n75\n500\n")

    clickhouse_node.query("DROP DATABASE alter_enum8_to_enum16_test")
    mysql_node.query("DROP DATABASE alter_enum8_to_enum16_test")

def move_to_prewhere_and_column_filtering(clickhouse_node, mysql_node, service_name):
    clickhouse_node.query("DROP DATABASE IF EXISTS cond_on_key_col")
    mysql_node.query("DROP DATABASE IF EXISTS cond_on_key_col")
    mysql_node.query("CREATE DATABASE cond_on_key_col")
    clickhouse_node.query("CREATE DATABASE cond_on_key_col ENGINE = MaterializeMySQL('{}:3306', 'cond_on_key_col', 'root', 'clickhouse')".format(service_name))
    mysql_node.query("create table cond_on_key_col.products (id int primary key, product_id int not null, catalog_id int not null, brand_id int not null, name text)")
    mysql_node.query("insert into cond_on_key_col.products (id, name, catalog_id, brand_id, product_id) values (915, 'ertyui', 5287, 15837, 0), (990, 'wer', 1053, 24390, 1), (781, 'qwerty', 1041, 1176, 2);")
    mysql_node.query("create table cond_on_key_col.test (id int(11) NOT NULL AUTO_INCREMENT, a int(11) DEFAULT NULL, b int(11) DEFAULT NULL, PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4;")
    mysql_node.query("insert into cond_on_key_col.test values (42, 123, 1);")
    mysql_node.query("CREATE TABLE cond_on_key_col.balance_change_record (id bigint(20) NOT NULL AUTO_INCREMENT, type tinyint(4) DEFAULT NULL, value decimal(10,4) DEFAULT NULL, time timestamp NULL DEFAULT NULL, "
                     "initiative_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, passivity_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, "
                     "person_id varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, tenant_code varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL, "
                     "created_time timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间', updated_time timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, "
                     "value_snapshot decimal(10,4) DEFAULT NULL, PRIMARY KEY (id), KEY balance_change_record_initiative_id (person_id) USING BTREE, "
                     "KEY type (type) USING BTREE, KEY balance_change_record_type (time) USING BTREE, KEY initiative_id (initiative_id) USING BTREE, "
                     "KEY balance_change_record_tenant_code (passivity_id) USING BTREE, KEY tenant_code (tenant_code) USING BTREE) ENGINE=InnoDB AUTO_INCREMENT=1691049 DEFAULT CHARSET=utf8")
    mysql_node.query("insert into cond_on_key_col.balance_change_record values (123, 1, 3.14, null, 'qwe', 'asd', 'zxc', 'rty', null, null, 2.7);")
    mysql_node.query("CREATE TABLE cond_on_key_col.test1 (id int(11) NOT NULL AUTO_INCREMENT, c1 varchar(32) NOT NULL, c2 varchar(32), PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
    mysql_node.query("insert into cond_on_key_col.test1(c1,c2) values ('a','b'), ('c', null);")
    check_query(clickhouse_node, "SELECT DISTINCT P.id, P.name, P.catalog_id FROM cond_on_key_col.products P WHERE P.name ILIKE '%e%' and P.catalog_id=5287", '915\tertyui\t5287\n')
    check_query(clickhouse_node, "select count(a) from cond_on_key_col.test where b = 1;", "1\n")
    check_query(clickhouse_node, "select id from cond_on_key_col.balance_change_record where type=1;", "123\n")
    check_query(clickhouse_node, "select count(c1) from cond_on_key_col.test1 where c2='b';", "1\n")
    clickhouse_node.query("DROP DATABASE cond_on_key_col")
    mysql_node.query("DROP DATABASE cond_on_key_col")

def mysql_settings_test(clickhouse_node, mysql_node, service_name):
    mysql_node.query("DROP DATABASE IF EXISTS test_database")
    clickhouse_node.query("DROP DATABASE IF EXISTS test_database")
    mysql_node.query("CREATE DATABASE test_database")
    mysql_node.query("CREATE TABLE test_database.a (id INT(11) NOT NULL PRIMARY KEY, value VARCHAR(255))")
    mysql_node.query("INSERT INTO test_database.a VALUES(1, 'foo')")
    mysql_node.query("INSERT INTO test_database.a VALUES(2, 'bar')")

    clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))
    check_query(clickhouse_node, "SELECT COUNT() FROM test_database.a FORMAT TSV", "2\n")

    assert clickhouse_node.query("SELECT COUNT(DISTINCT  blockNumber()) FROM test_database.a FORMAT TSV") == "2\n"

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")

