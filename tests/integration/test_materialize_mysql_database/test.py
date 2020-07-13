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


def test_clickhouse_ddl_for_materialize_mysql_database(started_cluster):
    pass

def test_mysql_ddl_for_materialize_mysql_database(started_cluster):
    with contextlib.closing(MySQLNodeInstance('root', 'clickhouse', '127.0.0.1', port=3308)) as mysql_node:
        mysql_node.query("CREATE DATABASE test_database DEFAULT CHARACTER SET 'utf8'")

        clickhouse_node.query("CREATE DATABASE test_database ENGINE = MaterializeMySQL('mysql1:3306', 'test_database', 'root', 'clickhouse')")
        assert 'test_database' in clickhouse_node.query('SHOW DATABASES')

        mysql_node.query('CREATE TABLE `test_database`.`test_table_1` ( `id` int(11) NOT NULL, PRIMARY KEY (`id`) ) ENGINE = InnoDB;')
        assert 'test_table_1' in clickhouse_node.query('SHOW TABLES FROM test_database')

        mysql_node.query('CREATE TABLE `test_database`.`test_table_2` ( `id` int(11) NOT NULL PRIMARY KEY) ENGINE = InnoDB;')
        assert 'test_table_2' in clickhouse_node.query('SHOW TABLES FROM test_database')

        mysql_node.query('RENAME TABLE `test_table_2` TO `test_database`.`test_table_3`')
        assert 'test_table_3' in clickhouse_node.query('SHOW TABLES FROM test_database')
        assert 'test_table_2' not in clickhouse_node.query('SHOW TABLES FROM test_database')

        mysql_node.query('DROP TABLE `test_database`.`test_table_3`')
        assert 'test_table_3' not in clickhouse_node.query('SHOW TABLES FROM test_database')

        mysql_node.query('ALTER TABLE `test_database`.`test_table_1` ADD COLUMN `add_column` int(11) NOT NULL')
        assert 'add_column' in clickhouse_node.query("DESC `test_database`.`test_table_1`");
        #
        # time.sleep(3)  # Because the unit of MySQL modification time is seconds, modifications made in the same second cannot be obtained
        # mysql_node.query('ALTER TABLE `test_database`.`test_table` DROP COLUMN `add_column`')
        # assert 'add_column' not in clickhouse_node.query("SELECT name FROM system.columns WHERE table = 'test_table' AND database = 'test_database'")
        #
        # mysql_node.query('DROP TABLE `test_database`.`test_table`;')
        # assert 'test_table' not in clickhouse_node.query('SHOW TABLES FROM test_database')
        #
        # mysql_node.query("DROP DATABASE test_database")

        # TODO support
        # clickhouse_node.query("DROP DATABASE test_database")
        # assert 'test_database' not in clickhouse_node.query('SHOW DATABASES')