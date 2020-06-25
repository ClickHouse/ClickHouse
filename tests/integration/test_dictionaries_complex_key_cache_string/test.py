import pytest
import os
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
cluster = ClickHouseCluster(__file__, base_configs_dir=os.path.join(SCRIPT_DIR, 'configs'))

node = cluster.add_instance('node', main_configs=['configs/dictionaries/complex_key_cache_string.xml'])

@pytest.mark.parametrize("placement", ["memory", "ssd"])
@pytest.fixture(scope="module")
def started_cluster(placement):
    try:
        cluster.start()
        node.query("create table radars_table (radar_id String, radar_ip String, client_id String) engine=MergeTree() order by radar_id")

        yield cluster
    finally:
        cluster.shutdown()


def test_memory_consumption(started_cluster):
    node.query("insert into radars_table select toString(rand() % 5000), '{0}', '{0}' from numbers(1000)".format('w' * 8))
    node.query("insert into radars_table select toString(rand() % 5000), '{0}', '{0}' from numbers(1000)".format('x' * 16))
    node.query("insert into radars_table select toString(rand() % 5000), '{0}', '{0}' from numbers(1000)".format('y' * 32))
    node.query("insert into radars_table select toString(rand() % 5000), '{0}', '{0}' from numbers(1000)".format('z' * 64))

    # Fill dictionary
    node.query("select dictGetString('radars', 'client_id', tuple(toString(number))) from numbers(0, 5000)")

    allocated_first = int(node.query("select bytes_allocated from system.dictionaries where name = 'radars'").strip())

    alloc_array = []
    for i in xrange(5):
        node.query("select dictGetString('radars', 'client_id', tuple(toString(number))) from numbers(0, 5000)")

        allocated = int(node.query("select bytes_allocated from system.dictionaries where name = 'radars'").strip())
        alloc_array.append(allocated)

    # size doesn't grow
    assert all(allocated_first >= a for a in alloc_array)

    for i in xrange(5):
        node.query("select dictGetString('radars', 'client_id', tuple(toString(number))) from numbers(0, 5000)")

        allocated = int(node.query("select bytes_allocated from system.dictionaries where name = 'radars'").strip())
        alloc_array.append(allocated)

    # size doesn't grow
    assert all(allocated_first >= a for a in alloc_array)
