import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

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

    freeze_result = TSV.toMat(node.query('''FREEZE TABLE test.table;'''))
    assert 3 == len(freeze_result)
    for row in freeze_result:
        backup_path = row[4]
        node.exec_in_container(
            ["bash", "-c", "test -d {}".format(backup_path)]
        )

    freeze_result = TSV.toMat(node.query('''FREEZE TABLE test.table WITH NAME 'custom-shadow-path';'''))
    assert 3 == len(freeze_result)
    for row in freeze_result:
        backup_path = row[4]
        assert 'custom%2Dshadow%2Dpath' in backup_path
        node.exec_in_container(
            ["bash", "-c", "test -d {}".format(backup_path)]
        )

    freeze_result = TSV.toMat(node.query('''FREEZE TABLE test.table PARTITION 2019;'''))
    assert 2 == len(freeze_result)

    freeze_result = TSV.toMat(node.query('''FREEZE TABLE test.table PARTITION 202001;'''))
    assert 1 == len(freeze_result)

    freeze_result = TSV.toMat(node.query('''FREEZE TABLE test.table PARTITION 999;'''))
    assert 0 == len(freeze_result)
