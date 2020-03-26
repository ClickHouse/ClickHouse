import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance('node1', user_configs=['configs/users_on_cluster.xml'])
node2 = cluster.add_instance('node2', user_configs=['configs/users_on_cluster.xml'])

distributed = cluster.add_instance('distributed', main_configs=['configs/remote_servers.xml'], user_configs=['configs/users_on_distributed.xml'])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in [node1, node2]:
            node.query("CREATE TABLE sometable(date Date, id UInt32, value Int32) ENGINE = MergeTree() ORDER BY id;")
            node.query("INSERT INTO sometable VALUES (toDate('2020-01-20'), 1, 1)")

        distributed.query("CREATE TABLE proxy (date Date, id UInt32, value Int32) ENGINE = Distributed(test_cluster, default, sometable);")
        distributed.query("CREATE TABLE sysproxy (name String, value String) ENGINE = Distributed(test_cluster, system, settings);")

        yield cluster

    finally:
        cluster.shutdown()


def test_shard_doesnt_throw_on_constraint_violation(started_cluster):
    query = "SELECT COUNT() FROM proxy"
    assert distributed.query(query) == '2\n'
    assert distributed.query(query, user = 'normal') == '2\n'
    assert distributed.query(query, user = 'wasteful') == '2\n'
    assert distributed.query(query, user = 'readonly') == '2\n'
    
    assert distributed.query(query, settings={"max_memory_usage": 40000000, "readonly": 2}) == '2\n'
    assert distributed.query(query, settings={"max_memory_usage": 3000000000, "readonly": 2}) == '2\n'

    query = "SELECT COUNT() FROM remote('node{1,2}', 'default', 'sometable')"
    assert distributed.query(query) == '2\n'
    assert distributed.query(query, user = 'normal') == '2\n'
    assert distributed.query(query, user = 'wasteful') == '2\n'


def test_shard_clamps_settings(started_cluster):
    query = "SELECT hostName() as host, name, value FROM sysproxy WHERE name = 'max_memory_usage' OR name = 'readonly' ORDER BY host, name, value"
    assert distributed.query(query) == 'node1\tmax_memory_usage\t99999999\n'\
                                       'node1\treadonly\t0\n'\
                                       'node2\tmax_memory_usage\t10000000000\n'\
                                       'node2\treadonly\t1\n'
    assert distributed.query(query, user = 'normal') == 'node1\tmax_memory_usage\t80000000\n'\
                                                        'node1\treadonly\t0\n'\
                                                        'node2\tmax_memory_usage\t10000000000\n'\
                                                        'node2\treadonly\t1\n'
    assert distributed.query(query, user = 'wasteful') == 'node1\tmax_memory_usage\t99999999\n'\
                                                          'node1\treadonly\t0\n'\
                                                          'node2\tmax_memory_usage\t10000000000\n'\
                                                          'node2\treadonly\t1\n'
    assert distributed.query(query, user = 'readonly') == 'node1\tmax_memory_usage\t99999999\n'\
                                                          'node1\treadonly\t1\n'\
                                                          'node2\tmax_memory_usage\t10000000000\n'\
                                                          'node2\treadonly\t1\n'

    assert distributed.query(query, settings={"max_memory_usage": 1}) == 'node1\tmax_memory_usage\t11111111\n'\
                                                                         'node1\treadonly\t0\n'\
                                                                         'node2\tmax_memory_usage\t10000000000\n'\
                                                                         'node2\treadonly\t1\n'
    assert distributed.query(query, settings={"max_memory_usage": 40000000, "readonly": 2}) == 'node1\tmax_memory_usage\t40000000\n'\
                                                                                               'node1\treadonly\t2\n'\
                                                                                               'node2\tmax_memory_usage\t10000000000\n'\
                                                                                               'node2\treadonly\t1\n'
    assert distributed.query(query, settings={"max_memory_usage": 3000000000, "readonly": 2}) == 'node1\tmax_memory_usage\t99999999\n'\
                                                                                                 'node1\treadonly\t2\n'\
                                                                                                 'node2\tmax_memory_usage\t10000000000\n'\
                                                                                                 'node2\treadonly\t1\n'
