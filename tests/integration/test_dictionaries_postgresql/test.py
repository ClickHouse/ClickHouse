import pytest
import time
import psycopg2

from helpers.cluster import ClickHouseCluster
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=['configs/config.xml', 'configs/postgres_dict.xml', 'configs/log_conf.xml'], with_postgres=True)

postgres_dict_table_template = """
    CREATE TABLE IF NOT EXISTS {} (
    id Integer NOT NULL, value Integer NOT NULL, PRIMARY KEY (id))
    """
click_dict_table_template = """
    CREATE TABLE IF NOT EXISTS `test`.`dict_table_{}` (
        `id` UInt64, `value` UInt32
    ) ENGINE = Dictionary({})
    """

def get_postgres_conn(database=False):
    if database == True:
        conn_string = "host='localhost' dbname='clickhouse' user='postgres' password='mysecretpassword'"
    else:
        conn_string = "host='localhost' user='postgres' password='mysecretpassword'"
    conn = psycopg2.connect(conn_string)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True
    return conn

def create_postgres_db(conn, name):
    cursor = conn.cursor()
    cursor.execute("CREATE DATABASE {}".format(name))

def create_postgres_table(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(postgres_dict_table_template.format(table_name))

def create_and_fill_postgres_table(table_name):
    conn = get_postgres_conn(True)
    create_postgres_table(conn, table_name)
    # Fill postgres table using clickhouse postgres table function and check
    table_func = '''postgresql('postgres1:5432', 'clickhouse', '{}', 'postgres', 'mysecretpassword')'''.format(table_name)
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
        postgres_conn = get_postgres_conn()
        node1.query("CREATE DATABASE IF NOT EXISTS test")
        print("postgres connected")
        create_postgres_db(postgres_conn, 'clickhouse')
        yield cluster

    finally:
        cluster.shutdown()


def test_load_dictionaries(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    table_name = 'test0'
    create_and_fill_postgres_table(table_name)
    create_dict(table_name)
    dict_name = 'dict0'

    node1.query("SYSTEM RELOAD DICTIONARIES")
    assert node1.query("SELECT count() FROM `test`.`dict_table_{}`".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT dictGetUInt32('{}', 'id', toUInt64(0))".format(dict_name)) == '0\n'
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(9999))".format(dict_name)) == '9999\n'
    cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))


def test_invalidate_query(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    table_name = 'test0'
    create_and_fill_postgres_table(table_name)

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


def test_postgres_scema(started_cluster):
    conn = get_postgres_conn(True)
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
