import time
import pytest
import random
import string
import os
import struct

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

def create_tables(name, nodes, node_settings, shard):
    for i, (node, settings) in enumerate(zip(nodes, node_settings)):
        node.query(
        '''
        CREATE TABLE {name}(date Date, id UInt32, s String, arr Array(Int32))
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/{name}', '{repl}')
        PARTITION BY toYYYYMM(date)
        ORDER BY id
        SETTINGS index_granularity = 64, index_granularity_bytes = {index_granularity_bytes}, 
        min_rows_for_wide_part = {min_rows_for_wide_part}, min_rows_for_compact_part = {min_rows_for_compact_part},
        in_memory_parts_enable_wal = 1
        '''.format(name=name, shard=shard, repl=i, **settings))

def create_tables_old_format(name, nodes, shard):
    for i, node in enumerate(nodes):
        node.query(
        '''
        CREATE TABLE {name}(date Date, id UInt32, s String, arr Array(Int32))
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/{name}', '{repl}', date, id, 64)
        '''.format(name=name, shard=shard, repl=i))

node1 = cluster.add_instance('node1', config_dir="configs", with_zookeeper=True)
node2 = cluster.add_instance('node2', config_dir="configs", with_zookeeper=True)

settings_default = {'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 512, 'min_rows_for_compact_part' : 0}
settings_compact_only = {'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 1000000, 'min_rows_for_compact_part' : 0}
settings_not_adaptive = {'index_granularity_bytes' : 0, 'min_rows_for_wide_part' : 512, 'min_rows_for_compact_part' : 0}

node3 = cluster.add_instance('node3', config_dir="configs", with_zookeeper=True)
node4 = cluster.add_instance('node4', config_dir="configs", main_configs=['configs/no_leader.xml'], with_zookeeper=True)

settings_compact = {'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 512, 'min_rows_for_compact_part' : 0}
settings_wide = {'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 0, 'min_rows_for_compact_part' : 0}

node5 = cluster.add_instance('node5', config_dir='configs', main_configs=['configs/compact_parts.xml'], with_zookeeper=True)
node6 = cluster.add_instance('node6', config_dir='configs', main_configs=['configs/compact_parts.xml'], with_zookeeper=True)

settings_in_memory = {'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 512, 'min_rows_for_compact_part' : 256}

node9 = cluster.add_instance('node9', config_dir="configs", with_zookeeper=True, stay_alive=True)
node10 = cluster.add_instance('node10', config_dir="configs", with_zookeeper=True, stay_alive=True)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        create_tables('polymorphic_table', [node1, node2], [settings_default, settings_default], "shard1")
        create_tables('compact_parts_only', [node1, node2], [settings_compact_only, settings_compact_only], "shard1")
        create_tables('non_adaptive_table', [node1, node2], [settings_not_adaptive, settings_default], "shard1")
        create_tables('polymorphic_table_compact', [node3, node4], [settings_compact, settings_wide], "shard2")
        create_tables('polymorphic_table_wide', [node3, node4], [settings_wide, settings_compact], "shard2")
        create_tables_old_format('polymorphic_table', [node5, node6], "shard3")
        create_tables('in_memory_table', [node9, node10], [settings_in_memory, settings_in_memory], "shard4")
        create_tables('wal_table', [node9, node10], [settings_in_memory, settings_in_memory], "shard4")

        yield cluster

    finally:
        cluster.shutdown()

@pytest.mark.parametrize(
    ('first_node', 'second_node'),
    [
        (node1, node2), # compact parts
        (node5, node6), # compact parts, old-format
    ]
)
def test_polymorphic_parts_basics(start_cluster, first_node, second_node):
    first_node.query("SYSTEM STOP MERGES")
    second_node.query("SYSTEM STOP MERGES")

    for size in [300, 300, 600]:
        insert_random_data('polymorphic_table', first_node, size)
    second_node.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)

    assert first_node.query("SELECT count() FROM polymorphic_table") == "1200\n"
    assert second_node.query("SELECT count() FROM polymorphic_table") == "1200\n"

    expected = "Compact\t2\nWide\t1\n"

    assert TSV(first_node.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'polymorphic_table' AND active GROUP BY part_type ORDER BY part_type")) == TSV(expected)
    assert TSV(second_node.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'polymorphic_table' AND active GROUP BY part_type ORDER BY part_type")) == TSV(expected)

    first_node.query("SYSTEM START MERGES")
    second_node.query("SYSTEM START MERGES")

    for _ in range(40):
        insert_random_data('polymorphic_table', first_node, 10)
        insert_random_data('polymorphic_table', second_node, 10)

    first_node.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)
    second_node.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)

    assert first_node.query("SELECT count() FROM polymorphic_table") == "2000\n"
    assert second_node.query("SELECT count() FROM polymorphic_table") == "2000\n"

    first_node.query("OPTIMIZE TABLE polymorphic_table FINAL")
    second_node.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)

    assert first_node.query("SELECT count() FROM polymorphic_table") == "2000\n"
    assert second_node.query("SELECT count() FROM polymorphic_table") == "2000\n"

    assert first_node.query("SELECT DISTINCT part_type FROM system.parts WHERE table = 'polymorphic_table' AND active") == "Wide\n"
    assert second_node.query("SELECT DISTINCT part_type FROM system.parts WHERE table = 'polymorphic_table' AND active") == "Wide\n"

    # Check alters and mutations also work
    first_node.query("ALTER TABLE polymorphic_table ADD COLUMN ss String")
    first_node.query("ALTER TABLE polymorphic_table UPDATE ss = toString(id) WHERE 1")

    second_node.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)

    first_node.query("SELECT count(ss) FROM polymorphic_table") == "2000\n"
    first_node.query("SELECT uniqExact(ss) FROM polymorphic_table") == "600\n"

    second_node.query("SELECT count(ss) FROM polymorphic_table") == "2000\n"
    second_node.query("SELECT uniqExact(ss) FROM polymorphic_table") == "600\n"

# Checks mostly that merge from compact part to compact part works.
def test_compact_parts_only(start_cluster):
    for i in range(20):
        insert_random_data('compact_parts_only', node1, 100)
        insert_random_data('compact_parts_only', node2, 100)

    node1.query("SYSTEM SYNC REPLICA compact_parts_only", timeout=20)
    node2.query("SYSTEM SYNC REPLICA compact_parts_only", timeout=20)

    assert node1.query("SELECT count() FROM compact_parts_only") == "4000\n"
    assert node2.query("SELECT count() FROM compact_parts_only") == "4000\n"

    assert node1.query("SELECT DISTINCT part_type FROM system.parts WHERE table = 'compact_parts_only' AND active") == "Compact\n"
    assert node2.query("SELECT DISTINCT part_type FROM system.parts WHERE table = 'compact_parts_only' AND active") == "Compact\n"

    node1.query("OPTIMIZE TABLE compact_parts_only FINAL")
    node2.query("SYSTEM SYNC REPLICA compact_parts_only", timeout=20)
    assert node2.query("SELECT count() FROM compact_parts_only") == "4000\n"

    expected = "Compact\t1\n"
    assert TSV(node1.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'compact_parts_only' AND active GROUP BY part_type ORDER BY part_type")) == TSV(expected)
    assert TSV(node2.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'compact_parts_only' AND active GROUP BY part_type ORDER BY part_type")) == TSV(expected)


# Check that follower replicas create parts of the same type, which leader has chosen at merge.
@pytest.mark.parametrize(
    ('table', 'part_type'),
    [
        ('polymorphic_table_compact', 'Compact'),
        ('polymorphic_table_wide', 'Wide')
    ]
)
def test_different_part_types_on_replicas(start_cluster, table, part_type):
    leader = node3
    follower = node4

    assert leader.query("SELECT is_leader FROM system.replicas WHERE table = '{}'".format(table)) == "1\n"
    assert node4.query("SELECT is_leader FROM system.replicas WHERE table = '{}'".format(table)) == "0\n"

    for _ in range(3):
        insert_random_data(table, leader, 100)

    leader.query("OPTIMIZE TABLE {} FINAL".format(table))
    follower.query("SYSTEM SYNC REPLICA {}".format(table), timeout=20)

    expected = "{}\t1\n".format(part_type)

    assert TSV(leader.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = '{}' AND active GROUP BY part_type ORDER BY part_type".format(table))) == TSV(expected)
    assert TSV(follower.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = '{}' AND active GROUP BY part_type ORDER BY part_type".format(table))) == TSV(expected)


node7 = cluster.add_instance('node7', config_dir="configs_old", with_zookeeper=True, image='yandex/clickhouse-server:19.17.8.54', stay_alive=True, with_installed_binary=True)
node8 = cluster.add_instance('node8', config_dir="configs", with_zookeeper=True)

settings7 = {'index_granularity_bytes' : 10485760}
settings8 = {'index_granularity_bytes' : 10485760, 'min_rows_for_wide_part' : 512, 'min_rows_for_compact_part' : 0}

@pytest.fixture(scope="module")
def start_cluster_diff_versions():
    try:
        for name in ['polymorphic_table', 'polymorphic_table_2']:
            cluster.start()
            node7.query(
            '''
            CREATE TABLE {name}(date Date, id UInt32, s String, arr Array(Int32))
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/shard5/{name}', '1')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
            SETTINGS index_granularity = 64, index_granularity_bytes = {index_granularity_bytes}
            '''.format(name=name, **settings7)
            )

            node8.query(
            '''
            CREATE TABLE {name}(date Date, id UInt32, s String, arr Array(Int32))
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/shard5/{name}', '2')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
            SETTINGS index_granularity = 64, index_granularity_bytes = {index_granularity_bytes},
            min_rows_for_wide_part = {min_rows_for_wide_part}, min_bytes_for_wide_part = {min_bytes_for_wide_part}
            '''.format(name=name, **settings8)
            )

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.skip(reason="compatability is temporary broken")
def test_polymorphic_parts_diff_versions(start_cluster_diff_versions):
    # Check that replication with Wide parts works between different versions.

    node_old = node7
    node_new = node8

    insert_random_data('polymorphic_table', node7, 100)
    node8.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)

    assert node8.query("SELECT count() FROM polymorphic_table") == "100\n"
    assert node8.query("SELECT DISTINCT part_type FROM system.parts WHERE table = 'polymorphic_table' and active") == "Wide\n"


@pytest.mark.skip(reason="compatability is temporary broken")
def test_polymorphic_parts_diff_versions_2(start_cluster_diff_versions):
    # Replication doesn't work on old version if part is created in compact format, because 
    #  this version doesn't know anything about it. It's considered to be ok.

    node_old = node7
    node_new = node8

    insert_random_data('polymorphic_table_2', node_new, 100)

    assert node_new.query("SELECT count() FROM polymorphic_table_2") == "100\n"
    assert node_old.query("SELECT count() FROM polymorphic_table_2") == "0\n"
    with pytest.raises(Exception):
        node_old.query("SYSTEM SYNC REPLICA polymorphic_table_2", timeout=3)

    node_old.restart_with_latest_version()

    node_old.query("SYSTEM SYNC REPLICA polymorphic_table_2", timeout=20)

    # Works after update
    assert node_old.query("SELECT count() FROM polymorphic_table_2") == "100\n"
    assert node_old.query("SELECT DISTINCT part_type FROM system.parts WHERE table = 'polymorphic_table_2' and active") == "Compact\n"


def test_polymorphic_parts_non_adaptive(start_cluster):
    node1.query("SYSTEM STOP MERGES")
    node2.query("SYSTEM STOP MERGES")

    insert_random_data('non_adaptive_table', node1, 100)
    node2.query("SYSTEM SYNC REPLICA non_adaptive_table", timeout=20)

    insert_random_data('non_adaptive_table', node2, 100)
    node1.query("SYSTEM SYNC REPLICA non_adaptive_table", timeout=20)

    assert TSV(node1.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'non_adaptive_table' AND active GROUP BY part_type ORDER BY part_type")) == TSV("Wide\t2\n")
    assert TSV(node2.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'non_adaptive_table' AND active GROUP BY part_type ORDER BY part_type")) == TSV("Wide\t2\n")

    assert node1.contains_in_log("<Warning> default.non_adaptive_table: Table can't create parts with adaptive granularity")

def test_in_memory(start_cluster):
    node9.query("SYSTEM STOP MERGES")
    node10.query("SYSTEM STOP MERGES")

    for size in [200, 200, 300, 600]:
        insert_random_data('in_memory_table', node9, size)
    node10.query("SYSTEM SYNC REPLICA in_memory_table", timeout=20)

    assert node9.query("SELECT count() FROM in_memory_table") == "1300\n"
    assert node10.query("SELECT count() FROM in_memory_table") == "1300\n"

    expected = "Compact\t1\nInMemory\t2\nWide\t1\n"

    assert TSV(node9.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'in_memory_table' AND active GROUP BY part_type ORDER BY part_type")) == TSV(expected)
    assert TSV(node10.query("SELECT part_type, count() FROM system.parts " \
        "WHERE table = 'in_memory_table' AND active GROUP BY part_type ORDER BY part_type")) == TSV(expected)

def test_in_memory_wal(start_cluster):
    node9.query("SYSTEM STOP MERGES")
    node10.query("SYSTEM STOP MERGES")

    for i in range(5):
        insert_random_data('wal_table', node9, 50)
    node10.query("SYSTEM SYNC REPLICA wal_table", timeout=20)

    assert node9.query("SELECT count() FROM wal_table") == "250\n"
    assert node10.query("SELECT count() FROM wal_table") == "250\n"

    assert node9.query("SELECT count() FROM system.parts WHERE table = 'wal_table' AND part_type = 'InMemory'") == '5\n'
    assert node10.query("SELECT count() FROM system.parts WHERE table = 'wal_table' AND part_type = 'InMemory'") == '5\n'

    # WAL works at inserts
    node9.restart_clickhouse(kill=True)
    time.sleep(5)
    assert node9.query("SELECT count() FROM wal_table") == "250\n"

    # WAL works at fetches
    node10.restart_clickhouse(kill=True)
    time.sleep(5)
    assert node10.query("SELECT count() FROM wal_table") == "250\n"

    node9.query("ALTER TABLE wal_table MODIFY SETTING in_memory_parts_enable_wal = 0")
    insert_random_data('wal_table', node9, 50)
    assert node9.query("SELECT count() FROM wal_table") == "300\n"

    # Data is lost without WAL
    node9.restart_clickhouse(kill=True)
    time.sleep(5)
    assert node9.query("SELECT count() FROM wal_table") == "250\n"

def test_polymorphic_parts_index(start_cluster):
    node1.query('''
        CREATE TABLE index_compact(a UInt32, s String) 
        ENGINE = MergeTree ORDER BY a 
        SETTINGS min_rows_for_wide_part = 1000, index_granularity = 128, merge_max_block_size = 100''')

    node1.query("INSERT INTO index_compact SELECT number, toString(number) FROM numbers(100)")
    node1.query("INSERT INTO index_compact SELECT number, toString(number) FROM numbers(30)")
    node1.query("OPTIMIZE TABLE index_compact FINAL")

    assert node1.query("SELECT part_type FROM system.parts WHERE table = 'index_compact' AND active") == "Compact\n"
    assert node1.query("SELECT marks FROM system.parts WHERE table = 'index_compact' AND active") == "2\n"

    index_path = os.path.join(node1.path, "database/data/default/index_compact/all_1_2_1/primary.idx")
    f = open(index_path, 'rb')

    assert os.path.getsize(index_path) == 8
    assert struct.unpack('I', f.read(4))[0] == 0
    assert struct.unpack('I', f.read(4))[0] == 99
