import time
import pytest
import random
import string

from helpers.test_tools import TSV
from helpers.test_tools import assert_eq_with_retry
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

def get_random_array():
    return [random.randint(0, 1000) % 1000 for _ in range(random.randint(0, 1000))]

def get_random_string():
    length = random.randint(0, 1000)
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(length))

def insert_random_data(table, node, size):
    data = [
    '(' + ','.join((
        "'2019-10-11'",
        str(i),
        "'" + get_random_string() + "'",
        str(get_random_array()))) +
    ')' for i in range(size)
    ]
    
    node.query("INSERT INTO {} VALUES {}".format(table, ','.join(data)))

def create_tables(nodes, node_settings, shard):
    for i, (node, settings) in enumerate(zip(nodes, node_settings)):
        node.query(
        '''
        CREATE TABLE polymorphic_table(date Date, id UInt32, s String, arr Array(Int32))
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{}/table_with_default_granularity', '{}')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
        SETTINGS index_granularity = {index_granularity}, index_granularity_bytes = {index_granularity_bytes}, 
        min_rows_for_wide_part = {min_rows_for_wide_part}, min_bytes_for_wide_part = {min_bytes_for_wide_part}
        '''.format(shard, i, **settings)
        )


node1 = cluster.add_instance('node1', config_dir="configs", main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node2 = cluster.add_instance('node2', config_dir="configs", main_configs=['configs/remote_servers.xml'], with_zookeeper=True)

settings1 = {'index_granularity' : 64, 'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 512, 'min_bytes_for_wide_part' : 0}

node3 = cluster.add_instance('node3', config_dir="configs", main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node4 = cluster.add_instance('node4', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/no_leader.xml'], with_zookeeper=True)

settings3 = {'index_granularity' : 64, 'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 512, 'min_bytes_for_wide_part' : 0}
settings4 = {'index_granularity' : 64, 'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 0, 'min_bytes_for_wide_part' : 0}

node5 = cluster.add_instance('node5', config_dir="configs", main_configs=['configs/remote_servers.xml'], with_zookeeper=True)
node6 = cluster.add_instance('node6', config_dir="configs", main_configs=['configs/remote_servers.xml', 'configs/no_leader.xml'], with_zookeeper=True)

settings5 = {'index_granularity' : 64, 'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 0, 'min_bytes_for_wide_part' : 0}
settings6 = {'index_granularity' : 64, 'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 512, 'min_bytes_for_wide_part' : 0}


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        create_tables([node1, node2], [settings1, settings1], "shard1")
        create_tables([node3, node4], [settings3, settings4], "shard2")
        create_tables([node5, node6], [settings5, settings6], "shard3")
        yield cluster

    finally:
        cluster.shutdown()


def test_polymorphic_parts_basics(start_cluster):
    node1.query("SYSTEM STOP MERGES")
    node2.query("SYSTEM STOP MERGES")

    for size in [300, 300, 600]:
        insert_random_data('polymorphic_table', node1, size)
    node2.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=10)

    assert node1.query("SELECT count() FROM polymorphic_table") == "1200\n"
    assert node2.query("SELECT count() FROM polymorphic_table") == "1200\n"

    expected = "Compact\t2\nWide\t1\n"

    assert TSV(node1.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'polymorphic_table' AND active GROUP BY part_type ORDER BY part_type")) == TSV(expected)
    assert TSV(node2.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'polymorphic_table' AND active GROUP BY part_type ORDER BY part_type")) == TSV(expected)

    node1.query("SYSTEM START MERGES")
    node2.query("SYSTEM START MERGES")

    for _ in range(40):
        insert_random_data('polymorphic_table', node1, 10)
        insert_random_data('polymorphic_table', node2, 10)

    node1.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=10)
    node2.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=10)

    assert node1.query("SELECT count() FROM polymorphic_table") == "2000\n"
    assert node2.query("SELECT count() FROM polymorphic_table") == "2000\n"

    node1.query("OPTIMIZE TABLE polymorphic_table FINAL")
    node2.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=10)

    assert node1.query("SELECT count() FROM polymorphic_table") == "2000\n"
    assert node2.query("SELECT count() FROM polymorphic_table") == "2000\n"

    assert node1.query("SELECT DISTINCT part_type FROM system.parts WHERE table = 'polymorphic_table' AND active") == "Wide\n"
    assert node2.query("SELECT DISTINCT part_type FROM system.parts WHERE table = 'polymorphic_table' AND active") == "Wide\n"


# Check that follower replicas create parts of the same type, which leader has chosen at merge.
@pytest.mark.parametrize(
    ('leader', 'follower', 'part_type'),
    [
        (node3, node4, 'Compact'),
        (node5, node6, 'Wide')
    ]
)
def test_different_part_types_on_replicas(start_cluster, leader, follower, part_type):
    assert leader.query("SELECT is_leader FROM system.replicas WHERE table = 'polymorphic_table'") == "1\n"
    assert follower.query("SELECT is_leader FROM system.replicas WHERE table = 'polymorphic_table'") == "0\n"

    for _ in  range(3):
        insert_random_data('polymorphic_table', leader, 100)

    leader.query("OPTIMIZE TABLE polymorphic_table FINAL")
    follower.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=10)

    expected = "{}\t1\n".format(part_type)

    assert TSV(leader.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'polymorphic_table' AND active GROUP BY part_type ORDER BY part_type")) == TSV(expected)
    assert TSV(follower.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'polymorphic_table' AND active GROUP BY part_type ORDER BY part_type")) == TSV(expected)
