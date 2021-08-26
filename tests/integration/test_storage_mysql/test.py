from contextlib import contextmanager

## sudo -H pip install PyMySQL
import pymysql.cursors
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_mysql=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_mysql_cluster=True)
node3 = cluster.add_instance('node3', main_configs=['configs/remote_servers.xml'], user_configs=['configs/users.xml'], with_mysql=True)

create_table_sql_template = """
    CREATE TABLE `clickhouse`.`{}` (
    `id` int(11) NOT NULL,
    `name` varchar(50) NOT NULL,
    `age` int  NOT NULL default 0,
    `money` int NOT NULL default 0,
    `source` enum('IP','URL') DEFAULT 'IP',
    PRIMARY KEY (`id`)) ENGINE=InnoDB;
    """

def get_mysql_conn(port=3308):
    conn = pymysql.connect(user='root', password='clickhouse', host='127.0.0.1', port=port)
    return conn


def create_mysql_db(conn, name):
    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))


def create_mysql_table(conn, tableName):
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql_template.format(tableName))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        ## create mysql db and table
        conn1 = get_mysql_conn(port=3308)
        create_mysql_db(conn1, 'clickhouse')
        yield cluster

    finally:
        cluster.shutdown()


def test_many_connections(started_cluster):
    table_name = 'test_many_connections'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse');
'''.format(table_name, table_name))

    node1.query("INSERT INTO {} (id, name) SELECT number, concat('name_', toString(number)) from numbers(10) ".format(table_name))

    query = "SELECT count() FROM ("
    for i in range (24):
        query += "SELECT id FROM {t} UNION ALL "
    query += "SELECT id FROM {t})"

    assert node1.query(query.format(t=table_name)) == '250\n'
    conn.close()


def test_insert_select(started_cluster):
    table_name = 'test_insert_select'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse');
'''.format(table_name, table_name))
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name))
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
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name))
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name))
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
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name))
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name))
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
    node1.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000) ".format(
            table_name))
    assert node1.query("SELECT count() FROM {} WHERE name LIKE '%name_%'".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT count() FROM {} WHERE name NOT LIKE '%tmp_%'".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT count() FROM {} WHERE money IN (1, 2, 3)".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT count() FROM {} WHERE money IN (1, 2, 4, 5, 6)".format(table_name)).rstrip() == '0'
    assert node1.query(
        "SELECT count() FROM {} WHERE money NOT IN (1, 2, 4, 5, 6)".format(table_name)).rstrip() == '10000'
    assert node1.query(
        "SELECT count() FROM {} WHERE name LIKE concat('name_', toString(1))".format(table_name)).rstrip() == '1'
    conn.close()


def test_table_function(started_cluster):
    conn = get_mysql_conn()
    create_mysql_table(conn, 'table_function')
    table_function = "mysql('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse')".format('table_function')
    assert node1.query("SELECT count() FROM {}".format(table_function)).rstrip() == '0'
    node1.query(
        "INSERT INTO {} (id, name, money) select number, concat('name_', toString(number)), 3 from numbers(10000)".format(
            'TABLE FUNCTION ' + table_function))
    assert node1.query("SELECT count() FROM {}".format(table_function)).rstrip() == '10000'
    assert node1.query("SELECT sum(c) FROM ("
                       "SELECT count() as c FROM {} WHERE id % 3 == 0"
                       " UNION ALL SELECT count() as c FROM {} WHERE id % 3 == 1"
                       " UNION ALL SELECT count() as c FROM {} WHERE id % 3 == 2)".format(table_function,
                                                                                          table_function,
                                                                                          table_function)).rstrip() == '10000'
    assert node1.query("SELECT sum(`money`) FROM {}".format(table_function)).rstrip() == '30000'
    node1.query("INSERT INTO {} (id, name, age, money) SELECT id + 100000, name, age, money FROM {}".format(
        'TABLE FUNCTION ' + table_function, table_function))
    assert node1.query("SELECT sum(`money`) FROM {}".format(table_function)).rstrip() == '60000'
    conn.close()


def test_binary_type(started_cluster):
    conn = get_mysql_conn()
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE clickhouse.binary_type (id INT PRIMARY KEY, data BINARY(16) NOT NULL)")
    table_function = "mysql('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse')".format('binary_type')
    node1.query("INSERT INTO {} VALUES (42, 'clickhouse')".format('TABLE FUNCTION ' + table_function))
    assert node1.query("SELECT * FROM {}".format(table_function)) == '42\tclickhouse\\0\\0\\0\\0\\0\\0\n'


def test_enum_type(started_cluster):
    table_name = 'test_enum_type'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)
    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32, source Enum8('IP' = 1, 'URL' = 2)) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse', 1);
'''.format(table_name, table_name))
    node1.query("INSERT INTO {} (id, name, age, money, source) VALUES (1, 'name', 0, 0, 'URL')".format(table_name))
    assert node1.query("SELECT source FROM {} LIMIT 1".format(table_name)).rstrip() == 'URL'
    conn.close()


def test_mysql_distributed(started_cluster):
    table_name = 'test_replicas'

    conn1 = get_mysql_conn(port=3348)
    conn2 = get_mysql_conn(port=3388)
    conn3 = get_mysql_conn(port=3368)
    conn4 = get_mysql_conn(port=3308)

    create_mysql_db(conn1, 'clickhouse')
    create_mysql_db(conn2, 'clickhouse')
    create_mysql_db(conn3, 'clickhouse')

    create_mysql_table(conn1, table_name)
    create_mysql_table(conn2, table_name)
    create_mysql_table(conn3, table_name)
    create_mysql_table(conn4, table_name)

    # Storage with with 3 replicas
    node2.query('''
        CREATE TABLE test_replicas
        (id UInt32, name String, age UInt32, money UInt32)
        ENGINE = MySQL(`mysql{2|3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse'); ''')

    # Fill remote tables with different data to be able to check
    nodes = [node1, node2, node2, node2]
    for i in range(1, 5):
        nodes[i-1].query('''
            CREATE TABLE test_replica{}
            (id UInt32, name String, age UInt32, money UInt32)
            ENGINE = MySQL(`mysql{}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');'''.format(i, i))
        nodes[i-1].query("INSERT INTO test_replica{} (id, name) SELECT number, 'host{}' from numbers(10) ".format(i, i))

    # test multiple ports parsing
    result = node2.query('''SELECT DISTINCT(name) FROM mysql(`mysql{1|2|3}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse'); ''')
    assert(result == 'host1\n' or result == 'host2\n' or result == 'host3\n')
    result = node2.query('''SELECT DISTINCT(name) FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse'); ''')
    assert(result == 'host1\n' or result == 'host2\n' or result == 'host3\n')

    # check all replicas are traversed
    query = "SELECT * FROM ("
    for i in range (3):
        query += "SELECT name FROM test_replicas UNION DISTINCT "
    query += "SELECT name FROM test_replicas)"

    result = node2.query(query)
    assert(result == 'host2\nhost3\nhost4\n')

    # Storage with with two shards, each has 2 replicas
    node2.query('''
        CREATE TABLE test_shards
        (id UInt32, name String, age UInt32, money UInt32)
        ENGINE = ExternalDistributed('MySQL', `mysql{1|2}:3306,mysql{3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse'); ''')

    # Check only one replica in each shard is used
    result = node2.query("SELECT DISTINCT(name) FROM test_shards ORDER BY name")
    assert(result == 'host1\nhost3\n')

    # check all replicas are traversed
    query = "SELECT name FROM ("
    for i in range (3):
        query += "SELECT name FROM test_shards UNION DISTINCT "
    query += "SELECT name FROM test_shards) ORDER BY name"
    result = node2.query(query)
    assert(result == 'host1\nhost2\nhost3\nhost4\n')

    # disconnect mysql1
    started_cluster.pause_container('mysql1')
    result = node2.query("SELECT DISTINCT(name) FROM test_shards ORDER BY name")
    started_cluster.unpause_container('mysql1')
    assert(result == 'host2\nhost4\n' or result == 'host3\nhost4\n')


def test_external_settings(started_cluster):
    table_name = 'test_external_settings'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    node3.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse');
'''.format(table_name, table_name))
    node3.query(
        "INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(100) ".format(
            table_name))
    assert node3.query("SELECT count() FROM {}".format(table_name)).rstrip() == '100'
    assert node3.query("SELECT sum(money) FROM {}".format(table_name)).rstrip() == '300'
    node3.query("select value from system.settings where name = 'max_block_size' FORMAT TSV") == "2\n"
    node3.query("select value from system.settings where name = 'external_storage_max_read_rows' FORMAT TSV") == "0\n"
    assert node3.query("SELECT COUNT(DISTINCT blockNumber()) FROM {} FORMAT TSV".format(table_name)) == '50\n'
    conn.close()


if __name__ == '__main__':
    with contextmanager(started_cluster)() as cluster:
        for name, instance in list(cluster.instances.items()):
            print(name, instance.ip_address)
        input("Cluster created, press any key to destroy...")
