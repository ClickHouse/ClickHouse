import os

import pytest
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__)
node_memory = cluster.add_instance('node_memory', dictionaries=['configs/dictionaries/complex_key_cache_string.xml'])
node_ssd = cluster.add_instance('node_ssd', dictionaries=['configs/dictionaries/ssd_complex_key_cache_string.xml'])

@pytest.fixture()
def started_cluster():
    try:
        cluster.start()
        node_memory.query(
            "create table radars_table (radar_id String, radar_ip String, client_id String) engine=MergeTree() order by radar_id")
        node_ssd.query(
            "create table radars_table (radar_id String, radar_ip String, client_id String) engine=MergeTree() order by radar_id")

        yield cluster
    finally:
        cluster.shutdown()

@pytest.mark.skip(reason="SSD cache test can run on disk only")
@pytest.mark.parametrize("type", ["memory", "ssd"])
def test_memory_consumption(started_cluster, type):
    node = started_cluster.instances[f'node_{type}']
    node.query(
        "insert into radars_table select toString(rand() % 5000), '{0}', '{0}' from numbers(1000)".format('w' * 8))
    node.query(
        "insert into radars_table select toString(rand() % 5000), '{0}', '{0}' from numbers(1000)".format('x' * 16))
    node.query(
        "insert into radars_table select toString(rand() % 5000), '{0}', '{0}' from numbers(1000)".format('y' * 32))
    node.query(
        "insert into radars_table select toString(rand() % 5000), '{0}', '{0}' from numbers(1000)".format('z' * 64))

    # Fill dictionary
    node.query("select dictGetString('radars', 'client_id', tuple(toString(number))) from numbers(0, 5000)")

    allocated_first = int(node.query("select bytes_allocated from system.dictionaries where name = 'radars'").strip())

    alloc_array = []
    for i in range(5):
        node.query("select dictGetString('radars', 'client_id', tuple(toString(number))) from numbers(0, 5000)")

        allocated = int(node.query("select bytes_allocated from system.dictionaries where name = 'radars'").strip())
        alloc_array.append(allocated)

    # size doesn't grow
    assert all(allocated_first >= a for a in alloc_array)

    for i in range(5):
        node.query("select dictGetString('radars', 'client_id', tuple(toString(number))) from numbers(0, 5000)")

        allocated = int(node.query("select bytes_allocated from system.dictionaries where name = 'radars'").strip())
        alloc_array.append(allocated)

    # size doesn't grow
    assert all(allocated_first >= a for a in alloc_array)
