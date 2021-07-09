import pytest
import time
import psycopg2
from multiprocessing.dummy import Pool

from helpers.cluster import ClickHouseCluster
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1',
                             main_configs=['configs/config.xml', 'configs/dictionaries/postgres_dict.xml'],
                             with_postgres=True, with_postgres_cluster=True)

postgres_dict_table_template = """
    CREATE TABLE IF NOT EXISTS {} (
    id Integer NOT NULL, value Integer NOT NULL, PRIMARY KEY (id))
    """
click_dict_table_template = """
    CREATE TABLE IF NOT EXISTS `test`.`dict_table_{}` (
        `id` UInt64, `value` UInt32
    ) ENGINE = Dictionary({})
    """

def get_postgres_conn(ip, port, database=False):
    if database == True:
        conn_string = "host={} port={} dbname='clickhouse' user='postgres' password='mysecretpassword'".format(ip, port)
    else:
        conn_string = "host={} port={} user='postgres' password='mysecretpassword'".format(ip, port)

    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True
    return conn

def create_postgres_db(conn, name):
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE {}".format(name))

def create_postgres_table(cursor, table_name):
    cursor.execute(postgres_dict_table_template.format(table_name))

def create_and_fill_postgres_table(cursor, table_name, port, host):
    create_postgres_table(cursor, table_name)
    # Fill postgres table using clickhouse postgres table function and check
    table_func = '''postgresql('{}:{}', 'clickhouse', '{}', 'postgres', 'mysecretpassword')'''.format(host, port, table_name)
    node1.query('''INSERT INTO TABLE FUNCTION {} SELECT number, number from numbers(10000)
            '''.format(table_func, table_name))
    result = node1.query("SELECT count() FROM {}".format(table_func))
    assert result.rstrip() == '10000'

def create_dict(table_name, index=0):
    node1.query(click_dict_table_template.format(table_name, 'dict' + str(index)))


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node1.query("CREATE DATABASE IF NOT EXISTS test")

        postgres_conn = get_postgres_conn(ip=cluster.postgres_ip, port=cluster.postgres_port)
        print("postgres1 connected")
        create_postgres_db(postgres_conn, 'clickhouse')

        postgres_conn = get_postgres_conn(ip=cluster.postgres2_ip, port=cluster.postgres_port)
        print("postgres2 connected")
        create_postgres_db(postgres_conn, 'clickhouse')

        yield cluster

    finally:
        cluster.shutdown()


def test_load_dictionaries(started_cluster):
    conn = get_postgres_conn(ip=started_cluster.postgres_ip, database=True, port=started_cluster.postgres_port)
    cursor = conn.cursor()
    table_name = 'test0'
    create_and_fill_postgres_table(cursor, table_name, port=started_cluster.postgres_port, host=started_cluster.postgres_ip)
    create_dict(table_name)
    dict_name = 'dict0'

    node1.query("SYSTEM RELOAD DICTIONARY {}".format(dict_name))
    assert node1.query("SELECT count() FROM `test`.`dict_table_{}`".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT dictGetUInt32('{}', 'id', toUInt64(0))".format(dict_name)) == '0\n'
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(9999))".format(dict_name)) == '9999\n'

    cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))
    node1.query("DROP TABLE IF EXISTS {}".format(table_name))
    node1.query("DROP DICTIONARY IF EXISTS {}".format(dict_name))


def test_invalidate_query(started_cluster):
    conn = get_postgres_conn(ip=started_cluster.postgres_ip, database=True, port=started_cluster.postgres_port)
    cursor = conn.cursor()
    table_name = 'test0'
    create_and_fill_postgres_table(cursor, table_name, port=started_cluster.postgres_port, host=started_cluster.postgres_ip)

    # invalidate query: SELECT value FROM test0 WHERE id = 0
    dict_name = 'dict0'
    create_dict(table_name)
    node1.query("SYSTEM RELOAD DICTIONARY {}".format(dict_name))
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(0))".format(dict_name)) ==  "0\n"
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(1))".format(dict_name)) ==  "1\n"

    # update should happen
    cursor.execute("UPDATE {} SET value=value+1 WHERE id = 0".format(table_name))
    while True:
        result = node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(0))".format(dict_name))
        if result != '0\n':
            break
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(0))".format(dict_name)) == '1\n'

    # no update should happen
    cursor.execute("UPDATE {} SET value=value*2 WHERE id != 0".format(table_name))
    time.sleep(5)
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(0))".format(dict_name)) == '1\n'
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(1))".format(dict_name)) == '1\n'

    # update should happen
    cursor.execute("UPDATE {} SET value=value+1 WHERE id = 0".format(table_name))
    time.sleep(5)
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(0))".format(dict_name)) == '2\n'
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(1))".format(dict_name)) == '2\n'

    node1.query("DROP TABLE IF EXISTS {}".format(table_name))
    node1.query("DROP DICTIONARY IF EXISTS {}".format(dict_name))
    cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))


def test_dictionary_with_replicas(started_cluster):
    conn1 = get_postgres_conn(ip=started_cluster.postgres_ip, port=started_cluster.postgres_port, database=True)
    cursor1 = conn1.cursor()
    conn2 = get_postgres_conn(ip=started_cluster.postgres2_ip, port=started_cluster.postgres_port, database=True)
    cursor2 = conn2.cursor()

    create_postgres_table(cursor1, 'test1')
    create_postgres_table(cursor2, 'test1')

    cursor1.execute('INSERT INTO test1 select i, i from generate_series(0, 99) as t(i);');
    cursor2.execute('INSERT INTO test1 select i, i from generate_series(100, 199) as t(i);');

    create_dict('test1', 1)
    result = node1.query("SELECT * FROM `test`.`dict_table_test1` ORDER BY id")

    # priority 0 - non running port
    assert node1.contains_in_log('PostgreSQLConnectionPool: Connection error*')

    # priority 1 - postgres2, table contains rows with values 100-200
    # priority 2 - postgres1, table contains rows with values 0-100
    expected = node1.query("SELECT number, number FROM numbers(100, 100)")
    assert(result == expected)

    cursor1.execute("DROP TABLE IF EXISTS test1")
    cursor2.execute("DROP TABLE IF EXISTS test1")

    node1.query("DROP TABLE IF EXISTS test1")
    node1.query("DROP DICTIONARY IF EXISTS dict1")


def test_postgres_scema(started_cluster):
    conn = get_postgres_conn(ip=started_cluster.postgres_ip, port=started_cluster.postgres_port, database=True)
    cursor = conn.cursor()

    cursor.execute('CREATE SCHEMA test_schema')
    cursor.execute('CREATE TABLE test_schema.test_table (id integer, value integer)')
    cursor.execute('INSERT INTO test_schema.test_table SELECT i, i FROM generate_series(0, 99) as t(i)')

    node1.query('''
    CREATE DICTIONARY postgres_dict (id UInt32, value UInt32)
    PRIMARY KEY id
    SOURCE(POSTGRESQL(
        port 5432
        host 'postgres1'
        user  'postgres'
        password 'mysecretpassword'
        db 'clickhouse'
        table 'test_schema.test_table'))
        LIFETIME(MIN 1 MAX 2)
        LAYOUT(HASHED());
    ''')

    result = node1.query("SELECT dictGetUInt32(postgres_dict, 'value', toUInt64(1))")
    assert(int(result.strip()) == 1)
    result = node1.query("SELECT dictGetUInt32(postgres_dict, 'value', toUInt64(99))")
    assert(int(result.strip()) == 99)
    node1.query("DROP DICTIONARY IF EXISTS postgres_dict")


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
