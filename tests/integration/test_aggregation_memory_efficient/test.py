import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1')
node2 = cluster.add_instance('node2')


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        for node in [node1, node2]:
            node.query(
                "create table da_memory_efficient_shard(A Int64, B Int64) Engine=MergeTree order by A partition by B % 2;")

        node1.query("insert into da_memory_efficient_shard select number, number from numbers(100000);")
        node2.query("insert into da_memory_efficient_shard select number + 100000, number from numbers(100000);")

        yield cluster

    finally:
        cluster.shutdown()


def test_remote(start_cluster):
    node1.query(
        "set distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes=1")
    res = node1.query(
        "select sum(a) from (SELECT B, uniqExact(A) a FROM remote('node{1,2}', default.da_memory_efficient_shard) GROUP BY B)")
    assert res == '200000\n'

    node1.query("set distributed_aggregation_memory_efficient = 0")
    res = node1.query(
        "select sum(a) from (SELECT B, uniqExact(A) a FROM remote('node{1,2}', default.da_memory_efficient_shard) GROUP BY B)")
    assert res == '200000\n'

    node1.query(
        "set distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes=1")
    res = node1.query(
        "SELECT fullHostName() AS h, uniqExact(A) AS a FROM remote('node{1,2}', default.da_memory_efficient_shard) GROUP BY h ORDER BY h;")
    assert res == 'node1\t100000\nnode2\t100000\n'

    node1.query("set distributed_aggregation_memory_efficient = 0")
    res = node1.query(
        "SELECT fullHostName() AS h, uniqExact(A) AS a FROM remote('node{1,2}', default.da_memory_efficient_shard) GROUP BY h ORDER BY h;")
    assert res == 'node1\t100000\nnode2\t100000\n'
