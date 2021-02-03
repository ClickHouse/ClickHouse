import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'])
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'])


# test reproducing issue https://github.com/ClickHouse/ClickHouse/issues/3162
@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node1, node2):
            node.query('''
CREATE TABLE local_test (
  t UInt64,
  date Date DEFAULT toDate(t/1000),
  shard UInt64,
  col1 String,
  col2 String
) ENGINE = MergeTree
PARTITION BY toRelativeDayNum(date)
ORDER BY (t)
SETTINGS index_granularity=8192
            ''')

            node.query('''
CREATE TABLE dist_test (
  t UInt64,
  shard UInt64,
  date Date MATERIALIZED toDate(t/1000),
  col1 String,
  col2 String
) Engine = Distributed(testcluster, default, local_test, shard)
            ''')

        yield cluster

    finally:
        cluster.shutdown()


def test(started_cluster):
    node1.query("INSERT INTO local_test (t, shard, col1, col2) VALUES (1000, 0, 'x', 'y')")
    node2.query("INSERT INTO local_test (t, shard, col1, col2) VALUES (1000, 1, 'foo', 'bar')")
    assert node1.query(
        "SELECT col1, col2 FROM dist_test WHERE (t < 3600000) AND (col1 = 'foo') ORDER BY t ASC") == "foo\tbar\n"
