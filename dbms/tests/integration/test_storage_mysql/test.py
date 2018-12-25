from contextlib import contextmanager

import pytest

## sudo -H pip install PyMySQL
import pymysql.cursors

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_mysql = True)
create_table_sql_template =   """
    CREATE TABLE `{}`.`{}` (
    `id` int(11) NOT NULL,
    `name` varchar(50) NOT NULL,
    `age` int  NOT NULL default 0,
    `money` int NOT NULL default 0,
    PRIMARY KEY (`id`)) ENGINE=InnoDB;
    """

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        conn = get_mysql_conn()
        ## create mysql db and table
        create_mysql_db(conn, 'clickhouse')
        yield cluster

    finally:
        cluster.shutdown()


def test_insert_select(started_cluster):
    table_name = 'test_insert_select'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse');
'''.format(table_name, table_name))
    node1.query("INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(table_name))
    assert node1.query("SELECT count() FROM {}".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT sum(money) FROM {}".format(table_name)).rstrip() == '30000'
    conn.close()


def test_replace_select(started_cluster):
    table_name = 'test_replace_select'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse', 1);
'''.format(table_name, table_name))
    node1.query("INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(table_name))
    node1.query("INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(table_name))
    assert node1.query("SELECT count() FROM {}".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT sum(money) FROM {}".format(table_name)).rstrip() == '30000'
    conn.close()


def test_insert_on_duplicate_select(started_cluster):
    table_name = 'test_insert_on_duplicate_select'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse', 0, 'update money = money + values(money)');
'''.format(table_name, table_name))
    node1.query("INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(table_name))
    node1.query("INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(table_name))
    assert node1.query("SELECT count() FROM {}".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT sum(money) FROM {}".format(table_name)).rstrip() == '60000'
    conn.close()

def test_where(started_cluster):
    table_name = 'test_where'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)
    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse');
'''.format(table_name, table_name))
    node1.query("INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(table_name))
    assert node1.query("SELECT count() FROM {} WHERE name LIKE '%name_%'".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT count() FROM {} WHERE name NOT LIKE '%tmp_%'".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT count() FROM {} WHERE money IN (1, 2, 3)".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT count() FROM {} WHERE money IN (1, 2, 4, 5, 6)".format(table_name)).rstrip() == '0'
    assert node1.query("SELECT count() FROM {} WHERE money NOT IN (1, 2, 4, 5, 6)".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT count() FROM {} WHERE name LIKE concat('name_', toString(1))".format(table_name)).rstrip() == '1'
    conn.close()

def test_config_setting(started_cluster):
    database_name = 'test_database'
    replace_table_name = 'replace_table_name'
    duplicate_table_name = 'duplicate_table_name'
    conn = get_mysql_conn()
    create_mysql_db(conn, database_name)
    create_mysql_table(conn, replace_table_name, database_name)
    create_mysql_table(conn, duplicate_table_name, database_name)

    node1.query("CREATE DATABASE {}".format(database_name))
    node1.query('''
CREATE TABLE {}.{}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', '{}', '{}', 'root', 'clickhouse', 1);
'''.format(database_name, replace_table_name, database_name, replace_table_name))
    node1.query("INSERT INTO {}.{}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(database_name, replace_table_name))
    node1.query("INSERT INTO {}.{}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(database_name, replace_table_name))
    assert node1.query("SELECT count() FROM {}.{}".format(database_name, replace_table_name)).rstrip() == '10000'
    assert node1.query("SELECT sum(money) FROM {}.{}".format(database_name, replace_table_name)).rstrip() == '30000'

    node1.query('''
CREATE TABLE {}.{}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', '{}', '{}', 'root', 'clickhouse', 1);
'''.format(database_name, duplicate_table_name, database_name, duplicate_table_name))
    node1.query("INSERT INTO {}.{}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(database_name, duplicate_table_name))
    node1.query("INSERT INTO {}.{}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(database_name, duplicate_table_name))
    assert node1.query("SELECT count() FROM {}.{}".format(database_name, duplicate_table_name)).rstrip() == '10000'
    assert node1.query("SELECT sum(money) FROM {}.{}".format(database_name, duplicate_table_name)).rstrip() == '60000'
    conn.close()



def test_create_storage_with_settings(started_cluster):
    table_name = 'test_mysql_setting'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)
    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL() SETTINGS remote_address = 'mysql1:3306', remote_database = 'clickhouse', remote_table_name = '{}', user = 'root', password = 'clickhouse';
'''.format(table_name, table_name))
    node1.query("INSERT INTO {}(id, name, money) SELECT number, concat('name_', toString(number)), 3 FROM numbers(10000) ".format(table_name))
    assert node1.query("SELECT count() FROM {}".format(table_name)).rstrip() == '10000'
    conn.close()

def test_mapping_database_and_table_name(started_cluster):
    table_name = 'test_mapping_database_and_table_name'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)
    node1.query("CREATE DATABASE clickhouse;")
    node1.query('''CREATE TABLE clickhouse.{}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL() SETTINGS remote_address = 'mysql1:3306', user = 'root', password = 'clickhouse';'''.format(table_name))
    node1.query("INSERT INTO clickhouse.{}(id, name, money) SELECT number, concat('name_', toString(number)), 3 FROM numbers(10000) ".format(table_name))
    assert node1.query("SELECT count() FROM clickhouse.{}".format(table_name)).rstrip() == '10000'
    conn.close()

def test_use_mysql_session_variables(started_cluster):
    table_name = 'test_use_mysql_session_variables'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)
    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL() SETTINGS remote_address = 'mysql1:3306', remote_database = 'clickhouse', remote_table_name = '{}', user = 'root', password = 'clickhouse', mysql_variable_bulk_insert_buffer_size = 8388608;
'''.format(table_name, table_name))
    assert node1.query("SELECT count() FROM {}".format(table_name)).rstrip() == '0'

    try:
        node1.query("DROP TABLE {}".format(table_name))
        node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL() SETTINGS remote_address = 'mysql1:3306', remote_database = 'clickhouse', remote_table_name = '{}', user = 'root', password = 'clickhouse', mysql_variable_xxx = 'AAA';
'''.format(table_name, table_name))
        node1.query("SELECT count() FROM {}".format(table_name))
        assert False
    except:
        assert True

    conn.close()


def get_mysql_conn():
    conn = pymysql.connect(user='root', password='clickhouse', host='127.0.0.1', port=3308)
    return conn

def create_mysql_db(conn, name):
    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))

def create_mysql_table(conn, tableName, database_name = "clickhouse"):
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql_template.format(database_name, tableName))


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
        for name, instance in cluster.instances.items():
            print name, instance.ip_address
        raw_input("Cluster created, press any key to destroy...")
