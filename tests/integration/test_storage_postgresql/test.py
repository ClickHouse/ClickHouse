import time

import pytest
import psycopg2
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', main_configs=[], with_postgres=True)

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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        postgres_conn = get_postgres_conn()
        print("postgres connected")
        create_postgres_db(postgres_conn, 'clickhouse')
        yield cluster

    finally:
        cluster.shutdown()


def test_postgres_conversions(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    table_name = 'test_types'
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS {} (
        a smallint, b integer, c bigint, d real, e double precision, f serial, g bigserial,
        h timestamp, i date, j numeric(5, 5), k decimal(5, 5))'''.format(table_name))
    node1.query('''
        INSERT INTO TABLE FUNCTION postgresql('postgres1:5432', 'clickhouse', '{}', 'postgres', 'mysecretpassword') VALUES
        (-32768, -2147483648, -9223372036854775808, 1.12345, 1.1234567890, 2147483647, 9223372036854775807, '2000-05-12 12:12:12', '2000-05-12', 0.2, 0.2)'''.format(table_name))
    result = node1.query('''
        SELECT * FROM postgresql('postgres1:5432', 'clickhouse', '{}', 'postgres', 'mysecretpassword')'''.format(table_name))
    assert(result == '-32768\t-2147483648\t-9223372036854775808\t1.12345\t1.123456789\t2147483647\t9223372036854775807\t2000-05-12 12:12:12\t2000-05-12\t0.20000\t0.20000\n')

    table_name = 'test_array_dimensions'
    cursor.execute(
        '''CREATE TABLE IF NOT EXISTS {} (a date[] NOT NULL, b integer[][][], c decimal(5, 2)[][][][][][])'''.format(table_name))
    result = node1.query('''
        DESCRIBE TABLE postgresql('postgres1:5432', 'clickhouse', '{}', 'postgres', 'mysecretpassword')'''.format(table_name))
    expected ='a\tArray(Date)\t\t\t\t\t\nb\tArray(Array(Array(Nullable(Int32))))\t\t\t\t\t\nc\tArray(Array(Array(Array(Array(Array(Nullable(Decimal(5, 2))))))))'
    assert(result.rstrip() == expected)

    node1.query('''
        INSERT INTO TABLE FUNCTION postgresql('postgres1:5432', 'clickhouse', '{}', 'postgres', 'mysecretpassword')
        VALUES (['2000-05-12', '2000-05-12'], [[[1, 1], [NULL, NULL]], [[3, 3], [3, 3]], [[4, 4], [5, 5]]], [[[[[[0.1], [0.2], [0.3]]]]]])'''.format(table_name))
    result = node1.query('''
        SELECT * FROM postgresql('postgres1:5432', 'clickhouse', '{}', 'postgres', 'mysecretpassword')'''.format(table_name))
    assert(result == '''['2000-05-12','2000-05-12']\t[[[1,1],[NULL,NULL]],[[3,3],[3,3]],[[4,4],[5,5]]]\t[[[[[[0.10],[0.20],[0.30]]]]]]\n''')


def test_postgres_select_insert(started_cluster):
    conn = get_postgres_conn(True)
    cursor = conn.cursor()
    table_name = 'test_many'
    table = '''postgresql('postgres1:5432', 'clickhouse', '{}', 'postgres', 'mysecretpassword')'''.format(table_name)
    cursor.execute('CREATE TABLE IF NOT EXISTS {} (a integer, b text, c integer)'.format(table_name))

    result = node1.query('''
        INSERT INTO TABLE FUNCTION {}
        SELECT number, concat('name_', toString(number)), 3 from numbers(10000)'''.format(table))
    check1 = "SELECT count() FROM {}".format(table)
    check2 = "SELECT Sum(c) FROM {}".format(table)
    check3 = "SELECT count(c) FROM {} WHERE a % 2 == 0".format(table)
    check4 = "SELECT count() FROM {} WHERE b LIKE concat('name_', toString(1))".format(table)
    assert (node1.query(check1)).rstrip() == '10000'
    assert (node1.query(check2)).rstrip() == '30000'
    assert (node1.query(check3)).rstrip() == '5000'
    assert (node1.query(check4)).rstrip() == '1'


if __name__ == '__main__':
    cluster.start()
    input("Cluster created, press any key to destroy...")
    cluster.shutdown()
