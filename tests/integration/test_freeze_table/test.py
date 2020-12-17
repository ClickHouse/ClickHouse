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
    node.query('''
        CREATE TABLE table_for_freeze
        (
          key UInt64,
          value String
        )
        ENGINE = MergeTree()
        ORDER BY key
        PARTITION BY key % 10;
        ''')
    node.query('''
        INSERT INTO table_for_freeze SELECT number, toString(number) from numbers(10);
        ''')

    freeze_result = TSV.toMat(node.query('''
        ALTER TABLE table_for_freeze
        FREEZE WITH NAME 'test_01417'
        FORMAT TSVWithNames
        SETTINGS alter_partition_verbose_result = 1;
        '''))
    assert 11 == len(freeze_result)
    path_col_ix = freeze_result[0].index('part_backup_path')
    for row in freeze_result[1:]:  # skip header
        part_backup_path = row[path_col_ix]
        node.exec_in_container(
            ["bash", "-c", "test -d {}".format(part_backup_path)]
        )

    freeze_result = TSV.toMat(node.query('''
        ALTER TABLE table_for_freeze
        FREEZE PARTITION '3'
        WITH NAME
        'test_01417_single_part'
        FORMAT TSVWithNames
        SETTINGS alter_partition_verbose_result = 1;
        '''))
    assert 2 == len(freeze_result)
    path_col_ix = freeze_result[0].index('part_backup_path')
    for row in freeze_result[1:]:  # skip header
        part_backup_path = row[path_col_ix]
        assert 'test_01417_single_part' in part_backup_path
        node.exec_in_container(
            ["bash", "-c", "test -d {}".format(part_backup_path)]
        )
