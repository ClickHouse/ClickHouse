import pytest
import time
import psycopg2
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
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

def create_and_fill_postgres_table(table_name, index=0):
    table_name = table_name + str(index)
    conn = get_postgres_conn(True)
    create_postgres_table(conn, table_name)
    # Fill postgres table using clickhouse postgres table function and check
    table_func = '''postgresql('postgres1:5432', 'clickhouse', '{}', 'postgres', 'mysecretpassword')'''.format(table_name)
    node1.query('''INSERT INTO TABLE FUNCTION {} SELECT number, number from numbers(10000)
            '''.format(table_func, table_name))
    result = node1.query("SELECT count() FROM {}".format(table_func))
    assert result.rstrip() == '10000'

def create_dict(table_name, index=0):
    node1.query(click_dict_table_template.format(table_name + str(index), 'dict' + str(index)))


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
    table_name = 'test'
    create_and_fill_postgres_table(table_name)
    create_dict(table_name)
    table_name += str(0)
    dict_name = 'dict0'

    node1.query("SYSTEM RELOAD DICTIONARIES")
    assert node1.query("SELECT count() FROM `test`.`dict_table_{}`".format(table_name)).rstrip() == '10000'
    assert node1.query("SELECT dictGetUInt32('{}', 'id', toUInt64(0))".format(dict_name)) == '0\n'
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(9999))".format(dict_name)) == '9999\n'
    cursor.execute("DROP TABLE IF EXISTS {}".format(table_name))


def test_invalidate_query(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    table_name = 'test'
    create_and_fill_postgres_table(table_name, 0)
    create_and_fill_postgres_table(table_name, 1)

    # this dict has no invalidate query
    dict_name = 'dict0'
    create_dict(table_name)
    node1.query("SYSTEM RELOAD DICTIONARIES")
    first_update_time = node1.query("SELECT last_successful_update_time FROM system.dictionaries WHERE name = '{}'".format(dict_name))
    time.sleep(4)
    second_update_time = node1.query("SELECT last_successful_update_time FROM system.dictionaries WHERE name = '{}'".format(dict_name))
    assert first_update_time != second_update_time

    # this dict has invalidate query: SELECT value FROM test1 WHERE id = 0
    dict_name = 'dict1'
    create_dict(table_name, 1)
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(1))".format(dict_name)) ==  "1\n"

    first_update_time = node1.query("SELECT last_successful_update_time FROM system.dictionaries WHERE name = '{}'".format(dict_name))
    time.sleep(4)
    second_update_time = node1.query("SELECT last_successful_update_time FROM system.dictionaries WHERE name = '{}'".format(table_name))
    assert first_update_time != second_update_time

    # no update should be made
    cursor.execute("UPDATE {} SET value=value*2 WHERE id > 0".format(table_name+str(1)))
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(1))".format(dict_name)) == '1\n'

    # update should happen
    cursor.execute("UPDATE {} SET value=value+1 WHERE id=0".format(table_name+str(1)))
    time.sleep(5)
    assert node1.query("SELECT dictGetUInt32('{}', 'value', toUInt64(1))".format(dict_name)) == '2\n'


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
