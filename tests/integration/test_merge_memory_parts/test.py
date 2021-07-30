import pytest
import time

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance', main_configs=['configs/merge_tree_settings.xml'])
q = instance.query
path_to_data = '/var/lib/clickhouse/'


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        q('CREATE DATABASE test')     # Different path in shadow/ with Atomic

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture
def partition_table_simple(started_cluster):
    q("DROP TABLE IF EXISTS test.t_num")
    q("CREATE TABLE test.t_num (id Int64) ENGINE=MergeTree() ORDER BY (id) ")
    yield

    q('DROP TABLE test.partition')


def test_part_type(partition_table_simple):
    q("insert into test.t_num select number from system.numbers limit 10")
    res = q("select count(1) from system.parts where database='test' and table='t_num' and part_type='InMemory'")
    assert res == '1'

    q("insert into test.t_num select number from system.numbers limit 10000")
    res = q("select count(1) from system.parts where database='test' and table='t_num' and part_type='Compact'")
    assert res == '1'

    q("insert into test.t_num select number from system.numbers limit 100000")
    res = q("select count(1) from system.parts where database='test' and table='t_num' and part_type='Wide'")
    assert res == '1'

def test_part_merge(partition_table_simple):
    # memory to memory
    for i in range(4):
        q(f"insert into test.t_num select number from system.numbers limit {i * 10}, 10")
    res = q("select count(1) from system.parts where database='test' and table='t_num' and part_type='InMemory'")
    assert res == '4'

    q(f"system stop merges test.t_num")
    for i in range(4, 5):
        q(f"insert into test.t_num select number from system.numbers limit {i * 10}, 10")
    q("system start merges test.t_num")
    time.sleep(1)
    res = q("select sum(rows), count(1) from system.parts where database='test' and table='t_num' and part_type='InMemory' group by part_type")
    assert TSV(res) == TSV("50\t1")

    # memory to compact
    q(f"system stop merges test.t_num")
    for i in range(5, 10):
        q(f"insert into test.t_num select number from system.numbers limit {i * 2000}, 2000")
    q("system start merges test.t_num")
    time.sleep(1)
    res = q("select count(1) from system.parts where database='test' and table='t_num' and part_type='Compact'")
    assert res == '1'
    res = q("select count(1) from system.parts where database='test' and table='t_num' and part_type='InMemory'")
    assert res == '0'

    # compact to wide and memory parts only get processed by Memory Selector.
    for i in range(10, 20):
        q(f"insert into test.t_num select number from system.numbers limit {i * 20000}, 20000")
    for i in range(20, 24):
        q(f"insert into test.t_num select number from system.numbers limit {i * 20}, 20000")

    q("optimize table test.t_num")
    time.sleep(1)
    res = q("select count(1) from system.parts where database='test' and table='t_num' and part_type='Wide'")
    assert res > 1
    res = q("select count(1) from system.parts where database='test' and table='t_num' and part_type='InMemory'")
    assert res == '4'










