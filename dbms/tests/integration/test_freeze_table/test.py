import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_freeze_table(started_cluster):
    node.query('CREATE DATABASE IF NOT EXISTS test')
    node.query('''
        CREATE TABLE test.table
        (
          d Date
        ) ENGINE = MergeTree(d, d, 8192);
        ''')
    node.query('''
        INSERT INTO
          test.table
        VALUES
          (toDate('2019-01-01')), (toDate('2019-02-01')), (toDate('2020-01-01'));
        ''')

    increment = int(node.query('''FREEZE TABLE test.table;'''))
    assert 3 == int(node.exec_in_container(
        ["bash", "-c", "ls -1 /var/lib/clickhouse/shadow/{}/data/test/table/ | wc -l".format(increment)]
    )), "expected 3 parts in backup"

    node.query('''FREEZE TABLE test.table WITH NAME 'custom-shadow-path';''')
    assert 3 == int(node.exec_in_container(
        ["bash", "-c", "ls -1 /var/lib/clickhouse/shadow/custom%2Dshadow%2Dpath/data/test/table/ | wc -l"]
    )), "expected 3 parts in backup"

    increment = int(node.query('''FREEZE TABLE test.table PARTITION 2019;'''))
    assert 2 == int(node.exec_in_container(
        ["bash", "-c", "ls -1 /var/lib/clickhouse/shadow/{}/data/test/table/ | wc -l".format(increment)]
    )), "expected 3 parts in backup"

    increment = int(node.query('''FREEZE TABLE test.table PARTITION 202001;'''))
    assert 1 == int(node.exec_in_container(
        ["bash", "-c", "ls -1 /var/lib/clickhouse/shadow/{}/data/test/table/ | wc -l".format(increment)]
    )), "expected 3 parts in backup"

    increment = int(node.query('''FREEZE TABLE test.table PARTITION 999;'''))
    node.exec_in_container(
        ["bash", "-c", "test ! -d /var/lib/clickhouse/shadow/{}/data/test/table/".format(increment)]
    )
