import time
import contextlib

import pymysql.cursors
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
clickhouse_node = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_mysql=True)

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

class MySQLNodeInstance:
    def __init__(self, user='root', password='clickhouse', hostname='127.0.0.1', port=3308):
        self.user = user
        self.port = port
        self.hostname = hostname
        self.password = password
        self.mysql_connection = None  # lazy init

    def query(self, execution_query):
        if self.mysql_connection is None:
            self.mysql_connection = pymysql.connect(user=self.user, password=self.password, host=self.hostname, port=self.port)
        with self.mysql_connection.cursor() as cursor:
            cursor.execute(execution_query)

    def close(self):
        if self.mysql_connection is not None:
            self.mysql_connection.close()


def check_query(query, result_set, retry_count=3, interval_seconds=1):
    for index in range(retry_count):
        if result_set == clickhouse_node.query(query):
            return
        time.sleep(interval_seconds)
    raise Exception("")


def test_mysql_drop_table_for_materialize_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

        mysql_node.query("DROP TABLE test_database.test_table_1;")

        mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

        mysql_node.query("TRUNCATE TABLE test_database.test_table_2;")

        # create mapping
        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")

        assert "test_database" in clickhouse_node.query("SHOW DATABASES")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_2")
        check_query("SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "")

        mysql_node.query("INSERT INTO test_database.test_table2 VALUES(1)(2)(3)(4)(5)(6)")
        mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
        check_query("SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "1\n2\n3\n4\n5\n6")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2")

        mysql_node.query("DROP TABLE test_database.test_table_1;")
        mysql_node.query("TRUNCATE TABLE test_database.test_table_2;")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_2")
        check_query("SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "")


def test_mysql_create_table_for_materialize_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        # existed before the mapping was created
        mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
        # it already has some data
        mysql_node.query("INSERT INTO test_database.test_table_1 VALUES(1)(2)(3)(5)(6)(7)")

        # create mapping
        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")

        # Check for pre-existing status
        assert "test_database" in clickhouse_node.query("SHOW DATABASES")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1")
        check_query("SELECT * FROM test_database.test_table_1 ORDER BY id FORMAT TSV", "1\n2\n3\n5\n6\n7")

        mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2")
        mysql_node.query("INSERT INTO test_database.test_table_2 VALUES(1)(2)(3)(4)(5)(6)")
        check_query("SELECT * FROM test_database.test_table_2 ORDER BY id FORMAT TSV", "1\n2\n3\n4\n5\n6")


def test_mysql_rename_table_for_materialize_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

        mysql_node.query("RENAME TABLE test_database.test_table_1 TO test_database.test_table_2")

        # create mapping
        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")

        assert "test_database" in clickhouse_node.query("SHOW DATABASES")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_2")
        mysql_node.query("RENAME TABLE test_database.test_table_2 TO test_database.test_table_1")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1")


def test_mysql_alter_add_column_for_materialize_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")

        mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_1 INT NOT NULL")
        mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_2 INT NOT NULL FIRST")
        mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_3 INT NOT NULL AFTER add_column_1")
        mysql_node.query("ALTER TABLE test_database.test_table_1 ADD COLUMN add_column_4 INT NOT NULL DEFAULT id")

        # create mapping
        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")

        assert "test_database" in clickhouse_node.query("SHOW DATABASES")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1")
        check_query("DESC test_database.test_table_1 FORMAT TSV", "add_column_2\tInt32\t\t\t\t\nid\tInt32\t\t\t\t\nadd_column_1\tInt32\t\t\t\t\nadd_column_3\tInt32\t\t\t\t")
        mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY) ENGINE = InnoDB;")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2")
        check_query("DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t")
        mysql_node.query("ALTER TABLE test_database.test_table_2 ADD COLUMN add_column_1 INT NOT NULL, ADD COLUMN add_column_2 INT NOT NULL FIRST")
        mysql_node.query("ALTER TABLE test_database.test_table_2 ADD COLUMN add_column_3 INT NOT NULL AFTER add_column_1, ADD COLUMN add_column_4 INT NOT NULL DEFAULT id")
        check_query("DESC test_database.test_table_2 FORMAT TSV", "add_column_2\tInt32\t\t\t\t\nid\tInt32\t\t\t\t\nadd_column_1\tInt32\t\t\t\t\nadd_column_3\tInt32\t\t\t\t")


def test_mysql_alter_drop_column_for_materialize_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY, drop_column INT) ENGINE = InnoDB;")

        mysql_node.query("ALTER TABLE test_database.test_table_1 DROP COLUMN drop_column")

        # create mapping
        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")

        assert "test_database" in clickhouse_node.query("SHOW DATABASES")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1")
        check_query("DESC test_database.test_table_1 FORMAT TSV", "id\tInt32\t\t\t\t")
        mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY, drop_column INT NOT NULL) ENGINE = InnoDB;")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2")
        check_query("DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\ndrop_column\tInt32\t\t\t\t")
        mysql_node.query("ALTER TABLE test_database.test_table_2 DROP COLUMN drop_column")
        check_query("DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t")


def test_mysql_alter_rename_column_for_materialize_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")

        # maybe should test rename primary key?
        mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY, rename_column INT NOT NULL) ENGINE = InnoDB;")

        mysql_node.query("ALTER TABLE test_database.test_table_1 RENAME COLUMN rename_column TO new_column_name")

        # create mapping
        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")

        assert "test_database" in clickhouse_node.query("SHOW DATABASES")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1")
        check_query("DESC test_database.test_table_1 FORMAT TSV", "id\tInt32\t\t\t\t\nnew_column_name\tInt32\t\t\t\t")
        mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY, rename_column INT NOT NULL) ENGINE = InnoDB;")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2")
        check_query("DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\nrename_column\tInt32\t\t\t\t")
        mysql_node.query("ALTER TABLE test_database.test_table_2 RENAME COLUMN rename_column TO new_column_name")
        check_query("DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\nnew_column_name\tInt32\t\t\t\t")


def test_mysql_alter_modify_column_for_materialize_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")

        # maybe should test rename primary key?
        mysql_node.query("CREATE TABLE test_database.test_table_1 (id INT NOT NULL PRIMARY KEY, modify_column INT NOT NULL) ENGINE = InnoDB;")

        mysql_node.query("ALTER TABLE test_database.test_table_1 MODIFY COLUMN modify_column INT")

        # create mapping
        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")

        assert "test_database" in clickhouse_node.query("SHOW DATABASES")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1")
        check_query("DESC test_database.test_table_1 FORMAT TSV", "id\tInt32\t\t\t\t\nmodify_column\tNullable(Int32)\t\t\t\t")
        mysql_node.query("CREATE TABLE test_database.test_table_2 (id INT NOT NULL PRIMARY KEY, modify_column INT NOT NULL) ENGINE = InnoDB;")
        check_query("SHOW TABLES FROM test_database FORMAT TSV", "test_table_1\ntest_table_2")
        check_query("DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\nmodify_column\tInt32\t\t\t\t")
        mysql_node.query("ALTER TABLE test_database.test_table_1 MODIFY COLUMN modify_column INT")
        check_query("DESC test_database.test_table_2 FORMAT TSV", "id\tInt32\t\t\t\t\nmodify_column\tNullable(Int32)\t\t\t\t")


# TODO: need support ALTER TABLE table_name ADD COLUMN column_name, RENAME COLUMN column_name TO new_column_name;
# def test_mysql_alter_change_column_for_materialize_mysql_database(started_cluster):
#     pass
