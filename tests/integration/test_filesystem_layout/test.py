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


def test_file_path_escaping(started_cluster):
    node.query('CREATE DATABASE IF NOT EXISTS test ENGINE = Ordinary')
    node.query('''
        CREATE TABLE test.`T.a_b,l-e!` (`~Id` UInt32)
        ENGINE = MergeTree() PARTITION BY `~Id` ORDER BY `~Id`;
        ''')
    node.query('''INSERT INTO test.`T.a_b,l-e!` VALUES (1);''')
    node.query('''ALTER TABLE test.`T.a_b,l-e!` FREEZE;''')

    node.exec_in_container(["bash", "-c", "test -f /var/lib/clickhouse/data/test/T%2Ea_b%2Cl%2De%21/1_1_1_0/%7EId.bin"])
    node.exec_in_container(["bash", "-c", "test -f /var/lib/clickhouse/shadow/1/data/test/T%2Ea_b%2Cl%2De%21/1_1_1_0/%7EId.bin"])
