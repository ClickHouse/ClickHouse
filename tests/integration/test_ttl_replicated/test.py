import time

import helpers.client as client
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_zookeeper=True)
node2 = cluster.add_instance('node2', with_zookeeper=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


def drop_table(nodes, table_name):
    for node in nodes:
        node.query("DROP TABLE IF EXISTS {} NO DELAY".format(table_name))

# Column TTL works only with wide parts, because it's very expensive to apply it for compact parts
def test_ttl_columns(started_cluster):
    drop_table([node1, node2], "test_ttl")
    for node in [node1, node2]:
        node.query(
            '''
                CREATE TABLE test_ttl(date DateTime, id UInt32, a Int32 TTL date + INTERVAL 1 DAY, b Int32 TTL date + INTERVAL 1 MONTH)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl_columns', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date) SETTINGS merge_with_ttl_timeout=0, min_bytes_for_wide_part=0;
            '''.format(replica=node.name))

    node1.query("INSERT INTO test_ttl VALUES (toDateTime('2000-10-10 00:00:00'), 1, 1, 3)")
    node1.query("INSERT INTO test_ttl VALUES (toDateTime('2000-10-11 10:00:00'), 2, 2, 4)")
    time.sleep(1)  # sleep to allow use ttl merge selector for second time
    node1.query("OPTIMIZE TABLE test_ttl FINAL")

    expected = "1\t0\t0\n2\t0\t0\n"
    assert TSV(node1.query("SELECT id, a, b FROM test_ttl ORDER BY id")) == TSV(expected)
    assert TSV(node2.query("SELECT id, a, b FROM test_ttl ORDER BY id")) == TSV(expected)


def test_merge_with_ttl_timeout(started_cluster):
    table = "test_merge_with_ttl_timeout"
    drop_table([node1, node2], table)
    for node in [node1, node2]:
        node.query(
            '''
                CREATE TABLE {table}(date DateTime, id UInt32, a Int32 TTL date + INTERVAL 1 DAY, b Int32 TTL date + INTERVAL 1 MONTH)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table}', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date)
                SETTINGS min_bytes_for_wide_part=0;
            '''.format(replica=node.name, table=table))

    node1.query("SYSTEM STOP TTL MERGES {table}".format(table=table))
    node2.query("SYSTEM STOP TTL MERGES {table}".format(table=table))

    for i in range(1, 4):
        node1.query(
            "INSERT INTO {table} VALUES (toDateTime('2000-10-{day:02d} 10:00:00'), 1, 2, 3)".format(day=i, table=table))

    assert node1.query("SELECT countIf(a = 0) FROM {table}".format(table=table)) == "0\n"
    assert node2.query("SELECT countIf(a = 0) FROM {table}".format(table=table)) == "0\n"

    node1.query("SYSTEM START TTL MERGES {table}".format(table=table))
    node2.query("SYSTEM START TTL MERGES {table}".format(table=table))

    time.sleep(15)  # TTL merges shall happen.

    for i in range(1, 4):
        node1.query(
            "INSERT INTO {table} VALUES (toDateTime('2000-10-{day:02d} 10:00:00'), 1, 2, 3)".format(day=i, table=table))

    time.sleep(15)  # TTL merges shall not happen.

    assert node1.query("SELECT countIf(a = 0) FROM {table}".format(table=table)) == "3\n"
    assert node2.query("SELECT countIf(a = 0) FROM {table}".format(table=table)) == "3\n"


def test_ttl_many_columns(started_cluster):
    drop_table([node1, node2], "test_ttl_2")
    for node in [node1, node2]:
        node.query(
            '''
                CREATE TABLE test_ttl_2(date DateTime, id UInt32,
                    a Int32 TTL date,
                    _idx Int32 TTL date,
                    _offset Int32 TTL date,
                    _partition Int32 TTL date)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl_2', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date) SETTINGS merge_with_ttl_timeout=0;
            '''.format(replica=node.name))

    node1.query("SYSTEM STOP TTL MERGES test_ttl_2")
    node2.query("SYSTEM STOP TTL MERGES test_ttl_2")

    node1.query("INSERT INTO test_ttl_2 VALUES (toDateTime('2000-10-10 00:00:00'), 1, 2, 3, 4, 5)")
    node1.query("INSERT INTO test_ttl_2 VALUES (toDateTime('2100-10-10 10:00:00'), 6, 7, 8, 9, 10)")

    node2.query("SYSTEM SYNC REPLICA test_ttl_2", timeout=5)

    # Check that part will appear in result of merge
    node1.query("SYSTEM STOP FETCHES test_ttl_2")
    node2.query("SYSTEM STOP FETCHES test_ttl_2")

    node1.query("SYSTEM START TTL MERGES test_ttl_2")
    node2.query("SYSTEM START TTL MERGES test_ttl_2")

    time.sleep(1)  # sleep to allow use ttl merge selector for second time
    node1.query("OPTIMIZE TABLE test_ttl_2 FINAL", timeout=5)

    node2.query("SYSTEM SYNC REPLICA test_ttl_2", timeout=5)

    expected = "1\t0\t0\t0\t0\n6\t7\t8\t9\t10\n"
    assert TSV(node1.query("SELECT id, a, _idx, _offset, _partition FROM test_ttl_2 ORDER BY id")) == TSV(expected)
    assert TSV(node2.query("SELECT id, a, _idx, _offset, _partition FROM test_ttl_2 ORDER BY id")) == TSV(expected)


@pytest.mark.parametrize("delete_suffix", [
    "",
    "DELETE",
])
def test_ttl_table(started_cluster, delete_suffix):
    drop_table([node1, node2], "test_ttl")
    for node in [node1, node2]:
        node.query(
            '''
                CREATE TABLE test_ttl(date DateTime, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date)
                TTL date + INTERVAL 1 DAY {delete_suffix} SETTINGS merge_with_ttl_timeout=0;
            '''.format(replica=node.name, delete_suffix=delete_suffix))

    node1.query("INSERT INTO test_ttl VALUES (toDateTime('2000-10-10 00:00:00'), 1)")
    node1.query("INSERT INTO test_ttl VALUES (toDateTime('2000-10-11 10:00:00'), 2)")
    time.sleep(1)  # sleep to allow use ttl merge selector for second time
    node1.query("OPTIMIZE TABLE test_ttl FINAL")

    assert TSV(node1.query("SELECT * FROM test_ttl")) == TSV("")
    assert TSV(node2.query("SELECT * FROM test_ttl")) == TSV("")


def test_modify_ttl(started_cluster):
    drop_table([node1, node2], "test_ttl")
    for node in [node1, node2]:
        node.query(
            '''
                CREATE TABLE test_ttl(d DateTime, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl_modify', '{replica}')
                ORDER BY id
            '''.format(replica=node.name))

    node1.query(
        "INSERT INTO test_ttl VALUES (now() - INTERVAL 5 HOUR, 1), (now() - INTERVAL 3 HOUR, 2), (now() - INTERVAL 1 HOUR, 3)")
    node2.query("SYSTEM SYNC REPLICA test_ttl", timeout=20)

    node1.query("ALTER TABLE test_ttl MODIFY TTL d + INTERVAL 4 HOUR SETTINGS mutations_sync = 2")
    assert node2.query("SELECT id FROM test_ttl") == "2\n3\n"

    node2.query("ALTER TABLE test_ttl MODIFY TTL d + INTERVAL 2 HOUR SETTINGS mutations_sync = 2")
    assert node1.query("SELECT id FROM test_ttl") == "3\n"

    node1.query("ALTER TABLE test_ttl MODIFY TTL d + INTERVAL 30 MINUTE SETTINGS mutations_sync = 2")
    assert node2.query("SELECT id FROM test_ttl") == ""


def test_modify_column_ttl(started_cluster):
    drop_table([node1, node2], "test_ttl")
    for node in [node1, node2]:
        node.query(
            '''
                CREATE TABLE test_ttl(d DateTime, id UInt32 DEFAULT 42)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl_column', '{replica}')
                ORDER BY d
            '''.format(replica=node.name))

    node1.query(
        "INSERT INTO test_ttl VALUES (now() - INTERVAL 5 HOUR, 1), (now() - INTERVAL 3 HOUR, 2), (now() - INTERVAL 1 HOUR, 3)")
    node2.query("SYSTEM SYNC REPLICA test_ttl", timeout=20)

    node1.query("ALTER TABLE test_ttl MODIFY COLUMN id UInt32 TTL d + INTERVAL 4 HOUR SETTINGS mutations_sync = 2")
    assert node2.query("SELECT id FROM test_ttl") == "42\n2\n3\n"

    node1.query("ALTER TABLE test_ttl MODIFY COLUMN id UInt32 TTL d + INTERVAL 2 HOUR SETTINGS mutations_sync = 2")
    assert node1.query("SELECT id FROM test_ttl") == "42\n42\n3\n"

    node1.query("ALTER TABLE test_ttl MODIFY COLUMN id UInt32 TTL d + INTERVAL 30 MINUTE SETTINGS mutations_sync = 2")
    assert node2.query("SELECT id FROM test_ttl") == "42\n42\n42\n"


def test_ttl_double_delete_rule_returns_error(started_cluster):
    drop_table([node1, node2], "test_ttl")
    try:
        node1.query('''
            CREATE TABLE test_ttl(date DateTime, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl_double_delete', '{replica}')
            ORDER BY id PARTITION BY toDayOfMonth(date)
            TTL date + INTERVAL 1 DAY, date + INTERVAL 2 DAY SETTINGS merge_with_ttl_timeout=0
        '''.format(replica=node1.name))
        assert False
    except client.QueryRuntimeException:
        pass
    except:
        assert False


def optimize_with_retry(node, table_name, retry=20):
    for i in range(retry):
        try:
            node.query("OPTIMIZE TABLE {name} FINAL SETTINGS optimize_throw_if_noop = 1".format(name=table_name), settings={"optimize_throw_if_noop": "1"})
            break
        except e:
            time.sleep(0.5)

@pytest.mark.parametrize("name,engine", [
    ("test_ttl_alter_delete", "MergeTree()"),
    ("test_replicated_ttl_alter_delete", "ReplicatedMergeTree('/clickhouse/test_replicated_ttl_alter_delete', '1')"),
])
def test_ttl_alter_delete(started_cluster, name, engine):
    """Copyright 2019, Altinity LTD
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""
    """Check compatibility with old TTL delete expressions to make sure
    that:
    * alter modify of column's TTL delete expression works
    * alter to add new columns works
    * alter modify to add TTL delete expression to a a new column works
    for a table that has TTL delete expression defined but
    no explicit storage policy assigned.
    """
    drop_table([node1], name)

    node1.query(
        """
            CREATE TABLE {name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 + INTERVAL 1 DAY DELETE
            SETTINGS min_bytes_for_wide_part=0
        """.format(name=name, engine=engine))

    node1.query("""ALTER TABLE {name} MODIFY COLUMN s1 String TTL d1 + INTERVAL 1 SECOND""".format(name=name))
    node1.query("""ALTER TABLE {name} ADD COLUMN b1 Int32""".format(name=name))

    node1.query("""INSERT INTO {name} (s1, b1, d1) VALUES ('hello1', 1, toDateTime({time}))""".format(name=name,
                                                                                                      time=time.time()))
    node1.query("""INSERT INTO {name} (s1, b1, d1) VALUES ('hello2', 2, toDateTime({time}))""".format(name=name,
                                                                                                      time=time.time() + 360))

    time.sleep(1)

    optimize_with_retry(node1, name)
    r = node1.query("SELECT s1, b1 FROM {name} ORDER BY b1, s1".format(name=name)).splitlines()
    assert r == ["\t1", "hello2\t2"]

    node1.query("""ALTER TABLE {name} MODIFY COLUMN b1 Int32 TTL d1""".format(name=name))
    node1.query("""INSERT INTO {name} (s1, b1, d1) VALUES ('hello3', 3, toDateTime({time}))""".format(name=name,
                                                                                                      time=time.time()))

    time.sleep(1)

    optimize_with_retry(node1, name)

    r = node1.query("SELECT s1, b1 FROM {name} ORDER BY b1, s1".format(name=name)).splitlines()
    assert r == ["\t0", "\t0", "hello2\t2"]

def test_ttl_empty_parts(started_cluster):
    drop_table([node1, node2], "test_ttl_empty_parts")
    for node in [node1, node2]:
        node.query(
        '''
            CREATE TABLE test_ttl_empty_parts(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl_empty_parts', '{replica}')
            ORDER BY id
            SETTINGS max_bytes_to_merge_at_min_space_in_pool = 1, max_bytes_to_merge_at_max_space_in_pool = 1,
                cleanup_delay_period = 1, cleanup_delay_period_random_add = 0
        '''.format(replica=node.name))

    for i in range (1, 7):
        node1.query("INSERT INTO test_ttl_empty_parts SELECT '2{}00-01-0{}', number FROM numbers(1000)".format(i % 2, i))

    assert node1.query("SELECT count() FROM test_ttl_empty_parts") == "6000\n"
    assert node1.query("SELECT name FROM system.parts WHERE table = 'test_ttl_empty_parts' AND active ORDER BY name") == \
        "all_0_0_0\nall_1_1_0\nall_2_2_0\nall_3_3_0\nall_4_4_0\nall_5_5_0\n"

    node1.query("ALTER TABLE test_ttl_empty_parts MODIFY TTL date")

    assert node1.query("SELECT count() FROM test_ttl_empty_parts") == "3000\n"

    time.sleep(3) # Wait for cleanup thread
    assert node1.query("SELECT name FROM system.parts WHERE table = 'test_ttl_empty_parts' AND active ORDER BY name") == \
        "all_0_0_0_6\nall_2_2_0_6\nall_4_4_0_6\n"

    for node in [node1, node2]:
        node.query("ALTER TABLE test_ttl_empty_parts MODIFY SETTING max_bytes_to_merge_at_min_space_in_pool = 1000000000")
        node.query("ALTER TABLE test_ttl_empty_parts MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1000000000")

    optimize_with_retry(node1, 'test_ttl_empty_parts')
    assert node1.query("SELECT name FROM system.parts WHERE table = 'test_ttl_empty_parts' AND active ORDER BY name") == "all_0_4_1_6\n"

    # Check that after removing empty parts mutations and merges works
    node1.query("INSERT INTO test_ttl_empty_parts SELECT '2100-01-20', number FROM numbers(1000)")
    node1.query("ALTER TABLE test_ttl_empty_parts DELETE WHERE id % 2 = 0 SETTINGS mutations_sync = 2")
    assert node1.query("SELECT count() FROM test_ttl_empty_parts") == "2000\n"

    optimize_with_retry(node1, 'test_ttl_empty_parts')
    assert node1.query("SELECT name FROM system.parts WHERE table = 'test_ttl_empty_parts' AND active ORDER BY name") == "all_0_7_2_8\n"

    node2.query('SYSTEM SYNC REPLICA test_ttl_empty_parts', timeout=20)

    error_msg = '<Error> default.test_ttl_empty_parts (ReplicatedMergeTreeCleanupThread)'
    assert not node1.contains_in_log(error_msg)
    assert not node2.contains_in_log(error_msg)
