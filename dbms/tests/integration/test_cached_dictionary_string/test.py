import pytest
import os
import time
from helpers.cluster import ClickHouseCluster
import random

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))

node = cluster.add_instance('node', main_configs=['configs/dictionaries/complex_key_cache_string.xml'])

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        node.query("create table radars_table (radar_id String, radar_ip String, client_id String) engine=MergeTree() order by radar_id")

        yield cluster
    finally:
        cluster.shutdown()


def test_memory_consumption(started_cluster):
    node.query("insert into radars_table select toString(number), 'xxxxxxxx', 'yyyyyyyy' from numbers(100000)")

    for i in xrange(30):
        node.query("select dictGetString('radars', 'client_id', tuple('{}'))".format(random.randint(0, 10000)))

    allocated_first = int(node.query("select bytes_allocated from system.dictionaries where name = 'radars'").strip())

    for i in xrange(100):
        node.query("select dictGetString('radars', 'client_id', tuple(toString(number))) from numbers({}, 1000)".format(random.randint(0, 10000)))

    allocated_second = int(node.query("select bytes_allocated from system.dictionaries where name = 'radars'").strip())
    one_element_size = allocated_first / 30 # number of elemnts in dict
    assert abs(allocated_first - allocated_second) <= one_element_size * 2 # less than two elements
