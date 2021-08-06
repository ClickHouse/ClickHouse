import time
import contextlib

import pymysql.cursors
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

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


def test_mysql_ddl_for_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")

        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")
        assert 'test_database' in clickhouse_node.query('SHOW DATABASES')

        mysql_node.query('CREATE TABLE `test_database`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;')
        assert 'test_table' in clickhouse_node.query('SHOW TABLES FROM test_database')

        time.sleep(3)  # Because the unit of MySQL modification time is seconds, modifications made in the same second cannot be obtained
        mysql_node.query('ALTER TABLE `test_database`.`test_table` ADD COLUMN `add_column` int(11)')
        assert 'add_column' in clickhouse_node.query("SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'")

        time.sleep(3)  # Because the unit of MySQL modification time is seconds, modifications made in the same second cannot be obtained
        mysql_node.query('ALTER TABLE `test_database`.`test_table` DROP COLUMN `add_column`')
        assert 'add_column' not in clickhouse_node.query("SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'")

        mysql_node.query('DROP TABLE `test_database`.`test_table`;')
        assert 'test_table' not in clickhouse_node.query('SHOW TABLES FROM test_database')

        clickhouse_node.query("DROP DATABASE test_database")
        assert 'test_database' not in clickhouse_node.query('SHOW DATABASES')

        mysql_node.query("DROP DATABASE test_database")


def test_clickhouse_ddl_for_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query('CREATE TABLE `test_database`.`test_table` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE=InnoDB;')

        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")

        assert 'test_table' in clickhouse_node.query('SHOW TABLES FROM test_database')
        clickhouse_node.query("DROP TABLE test_database.test_table")
        assert 'test_table' not in clickhouse_node.query('SHOW TABLES FROM test_database')
        clickhouse_node.query("ATTACH TABLE test_database.test_table")
        assert 'test_table' in clickhouse_node.query('SHOW TABLES FROM test_database')
        clickhouse_node.query("DETACH TABLE test_database.test_table")
        assert 'test_table' not in clickhouse_node.query('SHOW TABLES FROM test_database')
        clickhouse_node.query("ATTACH TABLE test_database.test_table")
        assert 'test_table' in clickhouse_node.query('SHOW TABLES FROM test_database')

        clickhouse_node.query("DROP DATABASE test_database")
        assert 'test_database' not in clickhouse_node.query('SHOW DATABASES')

        mysql_node.query("DROP DATABASE test_database")


def test_clickhouse_dml_for_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query('CREATE TABLE `test_database`.`test_table` ( `i``d` int(11) NOT NULL, PRIMARY KEY (`i``d`)) ENGINE=InnoDB;')
        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MySQL('mysql1:3306', test_database, 'root', 'clickhouse')")

        assert clickhouse_node.query("SELECT count() FROM `test_database`.`test_table`").rstrip() == '0'
        clickhouse_node.query("INSERT INTO `test_database`.`test_table`(`i\`d`) select number from numbers(10000)")
        assert clickhouse_node.query("SELECT count() FROM `test_database`.`test_table`").rstrip() == '10000'

        mysql_node.query("DROP DATABASE test_database")


def test_clickhouse_join_for_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE IF NOT EXISTS test DEFAULT CHARACTER SET 'utf8'")
        mysql_node.query("CREATE TABLE test.t1_mysql_local ("
                         "pays    VARCHAR(55) DEFAULT 'FRA' NOT NULL,"
                         "service VARCHAR(5)  DEFAULT ''    NOT NULL,"
                         "opco    CHAR(3)     DEFAULT ''    NOT NULL"
                         ")")
        mysql_node.query("CREATE TABLE test.t2_mysql_local ("
                         "service VARCHAR(5) DEFAULT '' NOT NULL,"
                         "opco    VARCHAR(5) DEFAULT ''"
                         ")")
        clickhouse_node.query("CREATE TABLE default.t1_remote_mysql AS mysql('mysql1:3306','test','t1_mysql_local','root','clickhouse')")
        clickhouse_node.query("CREATE TABLE default.t2_remote_mysql AS mysql('mysql1:3306','test','t2_mysql_local','root','clickhouse')")
        assert clickhouse_node.query("SELECT s.pays "
                                     "FROM default.t1_remote_mysql AS s "
                                     "LEFT JOIN default.t1_remote_mysql AS s_ref "
                                     "ON (s_ref.opco = s.opco AND s_ref.service = s.service)") == ''
        mysql_node.query("DROP DATABASE test")


def test_bad_arguments_for_mysql_database_engine(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        with pytest.raises(QueryRuntimeException) as exception:
            mysql_node.query("CREATE DATABASE IF NOT EXISTS test_bad_arguments DEFAULT CHARACTER SET 'utf8'")
            clickhouse_node.query("CREATE DATABASE test_database ENGINE = MySQL('mysql1:3306', test_bad_arguments, root, 'clickhouse')")

        assert 'Database engine MySQL requested literal argument.' in str(exception.value)
        mysql_node.query("DROP DATABASE test_bad_arguments")
