import pymongo

import pytest
from helpers.client import QueryRuntimeException

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', with_mongo=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


def get_mongo_connection():
    connection_str = 'mongodb://root:clickhouse@localhost:27018'
    return pymongo.MongoClient(connection_str)


def test_simple_select(started_cluster):
    mongo_connection = get_mongo_connection()
    db = mongo_connection['test']
    db.add_user('root', 'clickhouse')
    simple_mongo_table = db['simple_table']
    data = []
    for i in range(0, 100):
        data.append({'key': i, 'data': hex(i * i)})
    simple_mongo_table.insert_many(data)

    node.query(
        "CREATE TABLE simple_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'simple_table', 'root', 'clickhouse')")

    assert node.query("SELECT COUNT() FROM simple_mongo_table") == '100\n'
    assert node.query("SELECT sum(key) FROM simple_mongo_table") == str(sum(range(0, 100))) + '\n'

    assert node.query("SELECT data from simple_mongo_table where key = 42") == hex(42 * 42) + '\n'


def test_complex_data_type(started_cluster):
    mongo_connection = get_mongo_connection()
    db = mongo_connection['test']
    db.add_user('root', 'clickhouse')
    incomplete_mongo_table = db['complex_table']
    data = []
    for i in range(0, 100):
        data.append({'key': i, 'data': hex(i * i), 'dict': {'a': i, 'b': str(i)}})
    incomplete_mongo_table.insert_many(data)

    node.query(
        "CREATE TABLE incomplete_mongo_table(key UInt64, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'complex_table', 'root', 'clickhouse')")

    assert node.query("SELECT COUNT() FROM incomplete_mongo_table") == '100\n'
    assert node.query("SELECT sum(key) FROM incomplete_mongo_table") == str(sum(range(0, 100))) + '\n'

    assert node.query("SELECT data from incomplete_mongo_table where key = 42") == hex(42 * 42) + '\n'


def test_incorrect_data_type(started_cluster):
    mongo_connection = get_mongo_connection()
    db = mongo_connection['test']
    db.add_user('root', 'clickhouse')
    strange_mongo_table = db['strange_table']
    data = []
    for i in range(0, 100):
        data.append({'key': i, 'data': hex(i * i), 'aaaa': 'Hello'})
    strange_mongo_table.insert_many(data)

    node.query(
        "CREATE TABLE strange_mongo_table(key String, data String) ENGINE = MongoDB('mongo1:27017', 'test', 'strange_table', 'root', 'clickhouse')")

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT COUNT() FROM strange_mongo_table")

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT uniq(key) FROM strange_mongo_table")

    node.query(
        "CREATE TABLE strange_mongo_table2(key UInt64, data String, bbbb String) ENGINE = MongoDB('mongo1:27017', 'test', 'strange_table', 'root', 'clickhouse')")

    with pytest.raises(QueryRuntimeException):
        node.query("SELECT bbbb FROM strange_mongo_table2")
