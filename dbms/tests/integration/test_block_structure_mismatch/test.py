import time
import pytest

from contextlib import contextmanager
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', main_configs=['configs/remote_servers.xml'], with_zookeeper=True)

#test reproducing issue https://github.com/yandex/ClickHouse/issues/3162
@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        node1.query('''
CREATE TABLE local_test ON CLUSTER testcluster (
  t UInt64,
  date Date MATERIALIZED toDate(t/1000),
  shard UInt64,
  col1 String,
  col2 String
) ENGINE = MergeTree
PARTITION BY toRelativeDayNum(date)
ORDER BY (t)
SETTINGS index_granularity=8192
        ''')

        node1.query('''
CREATE TABLE dist_test ON CLUSTER testcluster (
  t UInt64,
  shard UInt64,
  date Date MATERIALIZED toDate(t/1000),
  col1 String,
  col2 String
) Engine = Distributed(testcluster, default, local_test, shard)
        ''')

        time.sleep(0.5)

        yield cluster

    finally:
        cluster.shutdown()

def test(started_cluster):
    node1.query("INSERT INTO dist_test (t, shard, col1, col2) VALUES (1000, 1, 'foo', 'bar'), (1000, 2, 'x', 'y')")
    time.sleep(3)
    assert node1.query("SELECT col1, col2 FROM dist_test WHERE (t < 3600000) AND (col1 = 'foo') ORDER BY t ASC") == "foo\tbar\n"
