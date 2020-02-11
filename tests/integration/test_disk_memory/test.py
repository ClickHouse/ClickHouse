import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=['configs/config.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_tinylog(started_cluster):
    node.query('''CREATE DATABASE IF NOT EXISTS test''')

    node.query('''CREATE TABLE test.tinylog (s String, n UInt8) ENGINE = TinyLog''')

    node.query('''INSERT INTO test.tinylog SELECT toString(number), number * 2 FROM system.numbers LIMIT 5''')
    assert TSV(node.query('''SELECT * FROM test.tinylog''')) == TSV('0\t0\n1\t2\n2\t4\n3\t6\n4\t8')

    node.query('''TRUNCATE TABLE test.tinylog''')
    assert TSV(node.query('''SELECT * FROM test.tinylog''')) == TSV('')

    node.query('''DROP TABLE test.tinylog''')
