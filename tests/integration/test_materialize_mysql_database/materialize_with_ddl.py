import time


def check_query(clickhouse_node, query, result_set, retry_count=3, interval_seconds=3):
    lastest_result = ''
    for index in range(retry_count):
        lastest_result = clickhouse_node.query(query)

        if result_set == lastest_result:
            return

        print lastest_result
        time.sleep(interval_seconds)

    assert lastest_result == result_set


def dml_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
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
        "_varchar VARCHAR(10), _char CHAR(10), "
        "/* Need ClickHouse support Enum('a', 'b', 'v') _enum ENUM('a', 'b', 'c'), */"
        "_date Date, _datetime DateTime, _timestamp TIMESTAMP, _bool BOOLEAN) ENGINE = InnoDB;")

    # it already has some data
    mysql_node.query(
        "INSERT INTO test_database.test_table_1 VALUES(1, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 3.2, -3.2, 3.4, -3.4, 'varchar', 'char', "
        "'2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00', true);")

    clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\n")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_1 ORDER BY key FORMAT TSV",
        "1\t1\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\tvarchar\tchar\t2020-01-01\t"
        "2020-01-01 00:00:00\t2020-01-01 00:00:00\t1\n")

    mysql_node.query(
        "INSERT INTO test_database.test_table_1 VALUES(2, 1, -1, 2, -2, 3, -3, 4, -4, 5, -5, 6, -6, 3.2, -3.2, 3.4, -3.4, 'varchar', 'char', "
        "'2020-01-01', '2020-01-01 00:00:00', '2020-01-01 00:00:00', false);")

    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_1 ORDER BY key FORMAT TSV",
        "1\t1\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\tvarchar\tchar\t2020-01-01\t"
        "2020-01-01 00:00:00\t2020-01-01 00:00:00\t1\n2\t1\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\t"
        "varchar\tchar\t2020-01-01\t2020-01-01 00:00:00\t2020-01-01 00:00:00\t0\n")

    mysql_node.query("UPDATE test_database.test_table_1 SET unsigned_tiny_int = 2 WHERE `key` = 1")

    check_query(clickhouse_node, "SELECT key, unsigned_tiny_int, tiny_int, unsigned_small_int,"
        " small_int, unsigned_medium_int, medium_int, unsigned_int, _int, unsigned_integer, _integer, "
        " unsigned_bigint, _bigint, unsigned_float, _float, unsigned_double, _double, _varchar, _char, "
        " _date, _datetime, /* exclude it, because ON UPDATE CURRENT_TIMESTAMP _timestamp, */ "
        " _bool FROM test_database.test_table_1 ORDER BY key FORMAT TSV",
        "1\t2\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\tvarchar\tchar\t2020-01-01\t"
        "2020-01-01 00:00:00\t1\n2\t1\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\t"
        "varchar\tchar\t2020-01-01\t2020-01-01 00:00:00\t0\n")

    # update primary key
    mysql_node.query("UPDATE test_database.test_table_1 SET `key` = 3 WHERE `unsigned_tiny_int` = 2")

    check_query(clickhouse_node, "SELECT key, unsigned_tiny_int, tiny_int, unsigned_small_int,"
        " small_int, unsigned_medium_int, medium_int, unsigned_int, _int, unsigned_integer, _integer, "
        " unsigned_bigint, _bigint, unsigned_float, _float, unsigned_double, _double, _varchar, _char, "
        " _date, _datetime, /* exclude it, because ON UPDATE CURRENT_TIMESTAMP _timestamp, */ "
        " _bool FROM test_database.test_table_1 ORDER BY key FORMAT TSV",
        "2\t1\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\t"
        "varchar\tchar\t2020-01-01\t2020-01-01 00:00:00\t0\n3\t2\t-1\t2\t-2\t3\t-3\t"
        "4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\tvarchar\tchar\t2020-01-01\t2020-01-01 00:00:00\t1\n")

    mysql_node.query('DELETE FROM test_database.test_table_1 WHERE `key` = 2')
    check_query(clickhouse_node, "SELECT key, unsigned_tiny_int, tiny_int, unsigned_small_int,"
        " small_int, unsigned_medium_int, medium_int, unsigned_int, _int, unsigned_integer, _integer, "
        " unsigned_bigint, _bigint, unsigned_float, _float, unsigned_double, _double, _varchar, _char, "
        " _date, _datetime, /* exclude it, because ON UPDATE CURRENT_TIMESTAMP _timestamp, */ "
        " _bool FROM test_database.test_table_1 ORDER BY key FORMAT TSV",
        "3\t2\t-1\t2\t-2\t3\t-3\t4\t-4\t5\t-5\t6\t-6\t3.2\t-3.2\t3.4\t-3.4\tvarchar\tchar\t2020-01-01\t"
        "2020-01-01 00:00:00\t1\n")

    mysql_node.query('DELETE FROM test_database.test_table_1 WHERE `unsigned_tiny_int` = 2')
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_1 ORDER BY key FORMAT TSV", "")

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")


def drop_table_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

    mysql_node.query("DROP TABLE test_database.test_table_1;")

    mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

    mysql_node.query("TRUNCATE TABLE test_database.test_table_2;")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_2\n")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "")

    mysql_node.query("INSERT INTO test_database.test_table_2 VALUES(1), (2), (3), (4), (5), (6)")
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "1\n2\n3\n4\n5\n6\n")

    mysql_node.query("DROP TABLE test_database.test_table_1;")
    mysql_node.query("TRUNCATE TABLE test_database.test_table_2;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_2\n")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "")

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")


def create_table_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
    # existed before the mapping was created
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
    # it already has some data
    mysql_node.query("INSERT INTO test_database.test_table_1 VALUES(1), (2), (3), (5), (6), (7);")

    # create mapping
    clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    # Check for pre-existing status
    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\n")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_1 ORDER BY id FORMAT TSV", "1\n2\n3\n5\n6\n7\n")

    mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
    mysql_node.query("INSERT INTO test_database.test_table_2 VALUES(1), (2), (3), (4), (5), (6);")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "1\n2\n3\n4\n5\n6\n")

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")


def rename_table_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

    mysql_node.query("RENAME TABLE test_database.test_table_1 TO test_database.test_table_2")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_2\n")
    mysql_node.query("RENAME TABLE test_database.test_table_2 TO test_database.test_table_1")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\n")

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")


def alter_add_column_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_1 INT NOT NULL")
    mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_2 INT NOT NULL FIRST")
    mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_3 INT NOT NULL AFTER add_column_1")
    mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_4 INT NOT NULL DEFAULT " + ("0" if service_name == "mysql5_7" else "(id)"))

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\n")
    check_query(clickhouse_node, "DESC test_database.test_table_1 FORMAT TSV",
        "add_column_2\tInt32\t\t\t\t\t\nid\tInt32\t\t\t\t\t\nadd_column_1\tInt32\t\t\t\t\t\nadd_column_3\tInt32\t\t\t\t\t\nadd_column_4\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_2 ADD COLUMN add_column_1 INT NOT NULL, ADD COLUMN add_column_2 INT NOT NULL FIRST")
    mysql_node.query(
        "ALTER TABLE test_database.test_table_2 ADD COLUMN add_column_3 INT NOT NULL AFTER add_column_1, ADD COLUMN add_column_4 INT NOT NULL DEFAULT " + (
            "0" if service_name == "mysql5_7" else "(id)"))

    default_expression = "DEFAULT\t0" if service_name == "mysql5_7" else "DEFAULT\tid"
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV",
        "add_column_2\tInt32\t\t\t\t\t\nid\tInt32\t\t\t\t\t\nadd_column_1\tInt32\t\t\t\t\t\nadd_column_3\tInt32\t\t\t\t\t\nadd_column_4\tInt32\t" + default_expression + "\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    mysql_node.query("INSERT INTO test_database.test_table_2 VALUES(1, 2, 3, 4, 5), (6, 7, 8, 9, 10)")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "1\t2\t3\t4\t5\n6\t7\t8\t9\t10\n")

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")


def alter_drop_column_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY, drop_column INT) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database.test_table_1 DROP COLUMN drop_column")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\n")
    check_query(clickhouse_node, "DESC test_database.test_table_1 FORMAT TSV", "id\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY, drop_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\ndrop_column\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_2 DROP COLUMN drop_column")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    mysql_node.query("INSERT INTO test_database.test_table_2 VALUES(1), (2), (3), (4), (5)")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "1\n2\n3\n4\n5\n")

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")


def alter_rename_column_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")

    # maybe should test rename primary key?
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY, rename_column INT NOT NULL) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database.test_table_1 RENAME COLUMN rename_column TO new_column_name")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\n")
    check_query(clickhouse_node, "DESC test_database.test_table_1 FORMAT TSV", "id\tInt32\t\t\t\t\t\nnew_column_name\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY, rename_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\nrename_column\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_2 RENAME COLUMN rename_column TO new_column_name")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\nnew_column_name\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    mysql_node.query("INSERT INTO test_database.test_table_2 VALUES(1, 2), (3, 4), (5, 6), (7, 8), (9, 10)")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "1\t2\n3\t4\n5\t6\n7\t8\n9\t10\n")

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")


def alter_modify_column_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")

    # maybe should test rename primary key?
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY, modify_column INT NOT NULL) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database.test_table_1 MODIFY COLUMN modify_column INT")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\n")
    check_query(clickhouse_node, "DESC test_database.test_table_1 FORMAT TSV", "id\tInt32\t\t\t\t\t\nmodify_column\tNullable(Int32)\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY, modify_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\nmodify_column\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_2 MODIFY COLUMN modify_column INT")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\nmodify_column\tNullable(Int32)\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_2 MODIFY COLUMN modify_column INT FIRST")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "modify_column\tNullable(Int32)\t\t\t\t\t\nid\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_2 MODIFY COLUMN modify_column INT AFTER id")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\nmodify_column\tNullable(Int32)\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    mysql_node.query("INSERT INTO test_database.test_table_2 VALUES(1, 2), (3, NULL)")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "1\t2\n3\t\\N\n")

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")


# TODO: need ClickHouse support ALTER TABLE table_name ADD COLUMN column_name, RENAME COLUMN column_name TO new_column_name;
# def test_mysql_alter_change_column_for_materialize_mysql_database(started_cluster):
#     pass

def alter_rename_table_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY, drop_column INT) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database.test_table_1 DROP COLUMN drop_column, RENAME TO test_database.test_table_2, RENAME TO test_database.test_table_3")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_3\n")
    check_query(clickhouse_node, "DESC test_database.test_table_3 FORMAT TSV", "id\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY, drop_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_3\n")
    check_query(clickhouse_node, "DESC test_database.test_table_1 FORMAT TSV", "id\tInt32\t\t\t\t\t\ndrop_column\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_1 DROP COLUMN drop_column, RENAME TO test_database.test_table_2, RENAME TO test_database.test_table_4")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_3\ntest_table_4\n")
    check_query(clickhouse_node, "DESC test_database.test_table_4 FORMAT TSV", "id\tInt32\t\t\t\t\t\n_sign\tInt8\tMATERIALIZED\t1\t\t\t\n_version\tUInt64\tMATERIALIZED\t1\t\t\t\n")

    mysql_node.query("INSERT INTO test_database.test_table_4 VALUES(1), (2), (3), (4), (5)")
    check_query(clickhouse_node, "SELECT * FROM test_database.test_table_4 ORDER BY id FORMAT TSV", "1\n2\n3\n4\n5\n")

    clickhouse_node.query("DROP DATABASE test_database")
    mysql_node.query("DROP DATABASE test_database")
