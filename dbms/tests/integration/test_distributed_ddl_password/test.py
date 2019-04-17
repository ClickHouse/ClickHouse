import time
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', config_dir="configs", with_zookeeper=True)
node2 = cluster.add_instance('node2', config_dir="configs", with_zookeeper=True)
node3 = cluster.add_instance('node3', config_dir="configs", with_zookeeper=True)
node4 = cluster.add_instance('node4', config_dir="configs", with_zookeeper=True)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        for node, shard in [(node1, 1), (node2, 1), (node3, 2), (node4, 2)]:
            node.query(
            '''
                CREATE TABLE test_table(date Date, id UInt32, dummy UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test{shard}/replicated', '{replica}')
                PARTITION BY date
                ORDER BY id
            '''.format(shard=shard, replica=node.name), settings={"password": "clickhouse"})

        yield cluster

    finally:
        cluster.shutdown()

def test_truncate(start_cluster):
    node1.query("insert into test_table values ('2019-02-15', 1, 2), ('2019-02-15', 2, 3), ('2019-02-15', 3, 4)", settings={"password": "clickhouse"})

    assert node1.query("select count(*) from test_table", settings={"password": "clickhouse"}) == "3\n"
    node2.query("system sync replica test_table", settings={"password": "clickhouse"})
    assert node2.query("select count(*) from test_table", settings={"password": "clickhouse"}) == "3\n"


    node3.query("insert into test_table values ('2019-02-16', 1, 2), ('2019-02-16', 2, 3), ('2019-02-16', 3, 4)", settings={"password": "clickhouse"})

    assert node3.query("select count(*) from test_table", settings={"password": "clickhouse"}) == "3\n"
    node4.query("system sync replica test_table", settings={"password": "clickhouse"})
    assert node4.query("select count(*) from test_table", settings={"password": "clickhouse"}) == "3\n"

    node3.query("truncate table test_table on cluster 'awesome_cluster'", settings={"password": "clickhouse"})
    time.sleep(2)

    for node in [node1, node2, node3, node4]:
        assert node.query("select count(*) from test_table", settings={"password": "clickhouse"}) == "0\n"

    node2.query("drop table test_table on cluster 'awesome_cluster'", settings={"password": "clickhouse"})
    time.sleep(2)

    for node in [node1, node2, node3, node4]:
        assert node.query("select count(*) from system.tables where name='test_table'", settings={"password": "clickhouse"}) == "0\n"
