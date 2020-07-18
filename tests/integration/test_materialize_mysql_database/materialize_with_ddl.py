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

    mysql_node.query("DROP DATABASE test_database")
    clickhouse_node.query("DROP DATABASE test_database")


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

    mysql_node.query("DROP DATABASE test_database")
    clickhouse_node.query("DROP DATABASE test_database")


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

    mysql_node.query("DROP DATABASE test_database")
    clickhouse_node.query("DROP DATABASE test_database")


def alter_add_column_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_1 INT NOT NULL")
    mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_2 INT NOT NULL FIRST")
    mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_3 INT NOT NULL AFTER add_column_1")
    mysql_node.query(
        "ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_4 INT NOT NULL DEFAULT " + ("0" if service_name == "mysql5_7" else "(id)"))

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\n")
    check_query(clickhouse_node, "DESC test_database.test_table_1 FORMAT TSV",
        "add_column_2\tInt32\t\t\t\t\t\nid\tInt32\t\t\t\t\t\nadd_column_1\tInt32\t\t\t\t\t\nadd_column_3\tInt32\t\t\t\t\t\nadd_column_4\tInt32\t\t\t\t\t\n")
    mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_2 ADD COLUMN add_column_1 INT NOT NULL, ADD COLUMN add_column_2 INT NOT NULL FIRST")
    mysql_node.query(
        "ALTER TABLE test_database.test_table_2 ADD COLUMN add_column_3 INT NOT NULL AFTER add_column_1, ADD COLUMN add_column_4 INT NOT NULL DEFAULT " + (
            "0" if service_name == "mysql5_7" else "(id)"))

    default_expression = "DEFAULT\t0" if service_name == "mysql5_7" else "DEFAULT\t(id)"
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV",
        "add_column_2\tInt32\t\t\t\t\t\nid\tInt32\t\t\t\t\t\nadd_column_1\tInt32\t\t\t\t\t\nadd_column_3\tInt32\t\t\t\t\t\nadd_column_4\tInt32\t" + default_expression + "\t\t\t\n")

    mysql_node.query("DROP DATABASE test_database")
    clickhouse_node.query("DROP DATABASE test_database")


def alter_drop_column_with_materialize_mysql_database(clickhouse_node, mysql_node, service_name):
    mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
    mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY, drop_column INT) ENGINE = InnoDB;")

    mysql_node.query("ALTER TABLE test_database.test_table_1 DROP COLUMN drop_column")

    # create mapping
    clickhouse_node.query(
        "CREATE DATABASE test_database ENGINE = MaterializeMySQL('{}:3306', 'test_database', 'root', 'clickhouse')".format(service_name))

    assert "test_database" in clickhouse_node.query("SHOW DATABASES")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\n")
    check_query(clickhouse_node, "DESC test_database.test_table_1 FORMAT TSV", "id\tInt32\t\t\t\t\t\n")
    mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY, drop_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\ndrop_column\tInt32\t\t\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_2 DROP COLUMN drop_column")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\n")

    mysql_node.query("DROP DATABASE test_database")
    clickhouse_node.query("DROP DATABASE test_database")


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
    check_query(clickhouse_node, "DESC test_database.test_table_1 FORMAT TSV", "id\tInt32\t\t\t\t\nnew_column_name\tInt32\t\t\t\t\n")
    mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY, rename_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\nrename_column\tInt32\t\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_2 RENAME COLUMN rename_column TO new_column_name")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\nnew_column_name\tInt32\t\t\t\t\n")

    mysql_node.query("DROP DATABASE test_database")
    clickhouse_node.query("DROP DATABASE test_database")


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
    check_query(clickhouse_node, "DESC test_database.test_table_1 FORMAT TSV", "id\tInt32\t\t\t\t\t\nmodify_column\tNullable(Int32)\t\t\t\t\t\n")
    mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY, modify_column INT NOT NULL) ENGINE = InnoDB;")
    check_query(clickhouse_node, "SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2\n")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\nmodify_column\tInt32\t\t\t\t\t\n")
    mysql_node.query("ALTER TABLE test_database.test_table_2 MODIFY COLUMN modify_column INT")
    check_query(clickhouse_node, "DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\t\nmodify_column\tNullable(Int32)\t\t\t\t\t\n")

    mysql_node.query("DROP DATABASE test_database")
    clickhouse_node.query("DROP DATABASE test_database")

# TODO: need support ALTER TABLE table_name ADD COLUMN column_name, RENAME COLUMN column_name TO new_column_name;
# def test_mysql_alter_change_column_for_materialize_mysql_database(started_cluster):
#     pass
