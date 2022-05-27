import os
import random
import string
import struct

import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from helpers.test_tools import assert_eq_with_retry, exec_query_with_retry

cluster = ClickHouseCluster(__file__)


def get_random_array():
    return [random.randint(0, 1000) % 1000 for _ in range(random.randint(0, 1000))]


def get_random_string():
    length = random.randint(0, 1000)
    return "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(length)
    )


def insert_random_data(table, node, size):
    data = [
        "("
        + ",".join(
            (
                "'2019-10-11'",
                str(i),
                "'" + get_random_string() + "'",
                str(get_random_array()),
            )
        )
        + ")"
        for i in range(size)
    ]

    node.query("INSERT INTO {} VALUES {}".format(table, ",".join(data)))


def create_tables(name, nodes, node_settings, shard):
    for i, (node, settings) in enumerate(zip(nodes, node_settings)):
        node.query(
            """
            CREATE TABLE {name}(date Date, id UInt32, s String, arr Array(Int32))
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/{name}', '{repl}')
            PARTITION BY toYYYYMM(date)
            ORDER BY id
            SETTINGS index_granularity = 64, index_granularity_bytes = {index_granularity_bytes},
            min_rows_for_wide_part = {min_rows_for_wide_part}, min_rows_for_compact_part = {min_rows_for_compact_part},
            min_bytes_for_wide_part = 0, min_bytes_for_compact_part = 0,
            in_memory_parts_enable_wal = 1
            """.format(
                name=name, shard=shard, repl=i, **settings
            )
        )


def create_tables_old_format(name, nodes, shard):
    for i, node in enumerate(nodes):
        node.query(
            """
            CREATE TABLE {name}(date Date, id UInt32, s String, arr Array(Int32))
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{shard}/{name}', '{repl}', date, id, 64)
            """.format(
                name=name, shard=shard, repl=i
            )
        )


node1 = cluster.add_instance(
    "node1",
    main_configs=[],
    user_configs=["configs/users.d/not_optimize_count.xml"],
    with_zookeeper=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[],
    user_configs=["configs/users.d/not_optimize_count.xml"],
    with_zookeeper=True,
)

settings_default = {
    "index_granularity_bytes": 10485760,
    "min_rows_for_wide_part": 512,
    "min_rows_for_compact_part": 0,
}
settings_compact_only = {
    "index_granularity_bytes": 10485760,
    "min_rows_for_wide_part": 1000000,
    "min_rows_for_compact_part": 0,
}
settings_not_adaptive = {
    "index_granularity_bytes": 0,
    "min_rows_for_wide_part": 512,
    "min_rows_for_compact_part": 0,
}

node3 = cluster.add_instance(
    "node3",
    main_configs=[],
    user_configs=["configs/users.d/not_optimize_count.xml"],
    with_zookeeper=True,
)
node4 = cluster.add_instance(
    "node4",
    user_configs=["configs/users.d/not_optimize_count.xml"],
    main_configs=["configs/no_leader.xml"],
    with_zookeeper=True,
)

settings_compact = {
    "index_granularity_bytes": 10485760,
    "min_rows_for_wide_part": 512,
    "min_rows_for_compact_part": 0,
}
settings_wide = {
    "index_granularity_bytes": 10485760,
    "min_rows_for_wide_part": 0,
    "min_rows_for_compact_part": 0,
}

node5 = cluster.add_instance(
    "node5", main_configs=["configs/compact_parts.xml"], with_zookeeper=True
)
node6 = cluster.add_instance(
    "node6", main_configs=["configs/compact_parts.xml"], with_zookeeper=True
)

settings_in_memory = {
    "index_granularity_bytes": 10485760,
    "min_rows_for_wide_part": 512,
    "min_rows_for_compact_part": 256,
}

node9 = cluster.add_instance("node9", with_zookeeper=True, stay_alive=True)
node10 = cluster.add_instance("node10", with_zookeeper=True)

node11 = cluster.add_instance(
    "node11",
    main_configs=["configs/do_not_merge.xml"],
    with_zookeeper=True,
    stay_alive=True,
)
node12 = cluster.add_instance(
    "node12",
    main_configs=["configs/do_not_merge.xml"],
    with_zookeeper=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        create_tables(
            "polymorphic_table",
            [node1, node2],
            [settings_default, settings_default],
            "shard1",
        )
        create_tables(
            "compact_parts_only",
            [node1, node2],
            [settings_compact_only, settings_compact_only],
            "shard1",
        )
        create_tables(
            "non_adaptive_table",
            [node1, node2],
            [settings_not_adaptive, settings_not_adaptive],
            "shard1",
        )
        create_tables(
            "polymorphic_table_compact",
            [node3, node4],
            [settings_compact, settings_wide],
            "shard2",
        )
        create_tables(
            "polymorphic_table_wide",
            [node3, node4],
            [settings_wide, settings_compact],
            "shard2",
        )
        create_tables_old_format("polymorphic_table", [node5, node6], "shard3")
        create_tables(
            "in_memory_table",
            [node9, node10],
            [settings_in_memory, settings_in_memory],
            "shard4",
        )
        create_tables(
            "wal_table",
            [node11, node12],
            [settings_in_memory, settings_in_memory],
            "shard4",
        )
        create_tables(
            "restore_table",
            [node11, node12],
            [settings_in_memory, settings_in_memory],
            "shard5",
        )
        create_tables(
            "deduplication_table",
            [node9, node10],
            [settings_in_memory, settings_in_memory],
            "shard5",
        )
        create_tables(
            "sync_table",
            [node9, node10],
            [settings_in_memory, settings_in_memory],
            "shard5",
        )
        create_tables(
            "alters_table",
            [node9, node10],
            [settings_in_memory, settings_in_memory],
            "shard5",
        )

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize(
    ("first_node", "second_node"),
    [
        (node1, node2),  # compact parts
        (node5, node6),  # compact parts, old-format
    ],
)
def test_polymorphic_parts_basics(start_cluster, first_node, second_node):
    first_node.query("SYSTEM STOP MERGES")
    second_node.query("SYSTEM STOP MERGES")

    for size in [300, 300, 600]:
        insert_random_data("polymorphic_table", first_node, size)
    second_node.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)

    assert first_node.query("SELECT count() FROM polymorphic_table") == "1200\n"
    assert second_node.query("SELECT count() FROM polymorphic_table") == "1200\n"

    expected = "Compact\t2\nWide\t1\n"

    assert TSV(
        first_node.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = 'polymorphic_table' AND active GROUP BY part_type ORDER BY part_type"
        )
    ) == TSV(expected)
    assert TSV(
        second_node.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = 'polymorphic_table' AND active GROUP BY part_type ORDER BY part_type"
        )
    ) == TSV(expected)

    first_node.query("SYSTEM START MERGES")
    second_node.query("SYSTEM START MERGES")

    for _ in range(40):
        insert_random_data("polymorphic_table", first_node, 10)
        insert_random_data("polymorphic_table", second_node, 10)

    first_node.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)
    second_node.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)

    assert first_node.query("SELECT count() FROM polymorphic_table") == "2000\n"
    assert second_node.query("SELECT count() FROM polymorphic_table") == "2000\n"

    first_node.query("OPTIMIZE TABLE polymorphic_table FINAL")
    second_node.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)

    assert first_node.query("SELECT count() FROM polymorphic_table") == "2000\n"
    assert second_node.query("SELECT count() FROM polymorphic_table") == "2000\n"

    assert (
        first_node.query(
            "SELECT DISTINCT part_type FROM system.parts WHERE table = 'polymorphic_table' AND active"
        )
        == "Wide\n"
    )
    assert (
        second_node.query(
            "SELECT DISTINCT part_type FROM system.parts WHERE table = 'polymorphic_table' AND active"
        )
        == "Wide\n"
    )

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
        insert_random_data("compact_parts_only", node1, 100)
        insert_random_data("compact_parts_only", node2, 100)

    node1.query("SYSTEM SYNC REPLICA compact_parts_only", timeout=20)
    node2.query("SYSTEM SYNC REPLICA compact_parts_only", timeout=20)

    assert node1.query("SELECT count() FROM compact_parts_only") == "4000\n"
    assert node2.query("SELECT count() FROM compact_parts_only") == "4000\n"

    assert (
        node1.query(
            "SELECT DISTINCT part_type FROM system.parts WHERE table = 'compact_parts_only' AND active"
        )
        == "Compact\n"
    )
    assert (
        node2.query(
            "SELECT DISTINCT part_type FROM system.parts WHERE table = 'compact_parts_only' AND active"
        )
        == "Compact\n"
    )

    node1.query("OPTIMIZE TABLE compact_parts_only FINAL")
    node2.query("SYSTEM SYNC REPLICA compact_parts_only", timeout=20)
    assert node2.query("SELECT count() FROM compact_parts_only") == "4000\n"

    expected = "Compact\t1\n"
    assert TSV(
        node1.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = 'compact_parts_only' AND active GROUP BY part_type ORDER BY part_type"
        )
    ) == TSV(expected)
    assert TSV(
        node2.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = 'compact_parts_only' AND active GROUP BY part_type ORDER BY part_type"
        )
    ) == TSV(expected)


# Check that follower replicas create parts of the same type, which leader has chosen at merge.
@pytest.mark.parametrize(
    ("table", "part_type"),
    [("polymorphic_table_compact", "Compact"), ("polymorphic_table_wide", "Wide")],
)
def test_different_part_types_on_replicas(start_cluster, table, part_type):
    leader = node3
    follower = node4

    assert (
        leader.query(
            "SELECT is_leader FROM system.replicas WHERE table = '{}'".format(table)
        )
        == "1\n"
    )
    assert (
        node4.query(
            "SELECT is_leader FROM system.replicas WHERE table = '{}'".format(table)
        )
        == "0\n"
    )

    for _ in range(3):
        insert_random_data(table, leader, 100)

    leader.query("OPTIMIZE TABLE {} FINAL".format(table))
    follower.query("SYSTEM SYNC REPLICA {}".format(table), timeout=20)

    expected = "{}\t1\n".format(part_type)

    assert TSV(
        leader.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = '{}' AND active GROUP BY part_type ORDER BY part_type".format(
                table
            )
        )
    ) == TSV(expected)
    assert TSV(
        follower.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = '{}' AND active GROUP BY part_type ORDER BY part_type".format(
                table
            )
        )
    ) == TSV(expected)


node7 = cluster.add_instance(
    "node7",
    user_configs=["configs_old/users.d/not_optimize_count.xml"],
    with_zookeeper=True,
    image="yandex/clickhouse-server",
    tag="19.17.8.54",
    stay_alive=True,
    with_installed_binary=True,
)
node8 = cluster.add_instance(
    "node8",
    user_configs=["configs/users.d/not_optimize_count.xml"],
    with_zookeeper=True,
)

settings7 = {"index_granularity_bytes": 10485760}
settings8 = {
    "index_granularity_bytes": 10485760,
    "min_rows_for_wide_part": 512,
    "min_rows_for_compact_part": 0,
}


@pytest.fixture(scope="module")
def start_cluster_diff_versions():
    try:
        for name in ["polymorphic_table", "polymorphic_table_2"]:
            cluster.start()
            node7.query(
                """
                CREATE TABLE {name}(date Date, id UInt32, s String, arr Array(Int32))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/shard5/{name}', '1')
                PARTITION BY toYYYYMM(date)
                ORDER BY id
                SETTINGS index_granularity = 64, index_granularity_bytes = {index_granularity_bytes}
                """.format(
                    name=name, **settings7
                )
            )

            node8.query(
                """
                CREATE TABLE {name}(date Date, id UInt32, s String, arr Array(Int32))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/shard5/{name}', '2')
                PARTITION BY toYYYYMM(date)
                ORDER BY id
                SETTINGS index_granularity = 64, index_granularity_bytes = {index_granularity_bytes},
                min_rows_for_wide_part = {min_rows_for_wide_part}, min_bytes_for_wide_part = {min_bytes_for_wide_part}
                """.format(
                    name=name, **settings8
                )
            )

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.skip(reason="compatability is temporary broken")
def test_polymorphic_parts_diff_versions(start_cluster_diff_versions):
    # Check that replication with Wide parts works between different versions.

    node_old = node7
    node_new = node8

    insert_random_data("polymorphic_table", node7, 100)
    node8.query("SYSTEM SYNC REPLICA polymorphic_table", timeout=20)

    assert node8.query("SELECT count() FROM polymorphic_table") == "100\n"
    assert (
        node8.query(
            "SELECT DISTINCT part_type FROM system.parts WHERE table = 'polymorphic_table' and active"
        )
        == "Wide\n"
    )


@pytest.mark.skip(reason="compatability is temporary broken")
def test_polymorphic_parts_diff_versions_2(start_cluster_diff_versions):
    # Replication doesn't work on old version if part is created in compact format, because
    #  this version doesn't know anything about it. It's considered to be ok.

    node_old = node7
    node_new = node8

    insert_random_data("polymorphic_table_2", node_new, 100)

    assert node_new.query("SELECT count() FROM polymorphic_table_2") == "100\n"
    assert node_old.query("SELECT count() FROM polymorphic_table_2") == "0\n"
    with pytest.raises(Exception):
        node_old.query("SYSTEM SYNC REPLICA polymorphic_table_2", timeout=3)

    node_old.restart_with_latest_version()

    node_old.query("SYSTEM SYNC REPLICA polymorphic_table_2", timeout=20)

    # Works after update
    assert node_old.query("SELECT count() FROM polymorphic_table_2") == "100\n"
    assert (
        node_old.query(
            "SELECT DISTINCT part_type FROM system.parts WHERE table = 'polymorphic_table_2' and active"
        )
        == "Compact\n"
    )


def test_polymorphic_parts_non_adaptive(start_cluster):
    node1.query("SYSTEM STOP MERGES")
    node2.query("SYSTEM STOP MERGES")

    insert_random_data("non_adaptive_table", node1, 100)
    node2.query("SYSTEM SYNC REPLICA non_adaptive_table", timeout=20)

    insert_random_data("non_adaptive_table", node2, 100)
    node1.query("SYSTEM SYNC REPLICA non_adaptive_table", timeout=20)

    assert TSV(
        node1.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = 'non_adaptive_table' AND active GROUP BY part_type ORDER BY part_type"
        )
    ) == TSV("Wide\t2\n")
    assert TSV(
        node2.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = 'non_adaptive_table' AND active GROUP BY part_type ORDER BY part_type"
        )
    ) == TSV("Wide\t2\n")

    assert node1.contains_in_log(
        "<Warning> default.non_adaptive_table ([0-9a-f-]*): Table can't create parts with adaptive granularity"
    )


def test_in_memory(start_cluster):
    node9.query("SYSTEM STOP MERGES")
    node10.query("SYSTEM STOP MERGES")

    for size in [200, 200, 300, 600]:
        insert_random_data("in_memory_table", node9, size)
    node10.query("SYSTEM SYNC REPLICA in_memory_table", timeout=20)

    assert node9.query("SELECT count() FROM in_memory_table") == "1300\n"
    assert node10.query("SELECT count() FROM in_memory_table") == "1300\n"

    expected = "Compact\t1\nInMemory\t2\nWide\t1\n"

    assert TSV(
        node9.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = 'in_memory_table' AND active GROUP BY part_type ORDER BY part_type"
        )
    ) == TSV(expected)
    assert TSV(
        node10.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = 'in_memory_table' AND active GROUP BY part_type ORDER BY part_type"
        )
    ) == TSV(expected)

    node9.query("SYSTEM START MERGES")
    node10.query("SYSTEM START MERGES")

    assert_eq_with_retry(
        node9,
        "OPTIMIZE TABLE in_memory_table FINAL SETTINGS optimize_throw_if_noop = 1",
        "",
    )
    node10.query("SYSTEM SYNC REPLICA in_memory_table", timeout=20)

    assert node9.query("SELECT count() FROM in_memory_table") == "1300\n"
    assert node10.query("SELECT count() FROM in_memory_table") == "1300\n"

    assert TSV(
        node9.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = 'in_memory_table' AND active GROUP BY part_type ORDER BY part_type"
        )
    ) == TSV("Wide\t1\n")
    assert TSV(
        node10.query(
            "SELECT part_type, count() FROM system.parts "
            "WHERE table = 'in_memory_table' AND active GROUP BY part_type ORDER BY part_type"
        )
    ) == TSV("Wide\t1\n")


def test_in_memory_wal_rotate(start_cluster):
    # Write every part to single wal
    node11.query(
        "ALTER TABLE restore_table MODIFY SETTING write_ahead_log_max_bytes = 10"
    )
    for i in range(5):
        insert_random_data("restore_table", node11, 50)

    for i in range(5):
        # Check file exists
        node11.exec_in_container(
            [
                "bash",
                "-c",
                "test -f /var/lib/clickhouse/data/default/restore_table/wal_{0}_{0}.bin".format(
                    i
                ),
            ]
        )

    for node in [node11, node12]:
        node.query(
            "ALTER TABLE restore_table MODIFY SETTING number_of_free_entries_in_pool_to_lower_max_size_of_merge = 0"
        )
        node.query(
            "ALTER TABLE restore_table MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 10000000"
        )

    assert_eq_with_retry(
        node11,
        "OPTIMIZE TABLE restore_table FINAL SETTINGS optimize_throw_if_noop = 1",
        "",
    )
    # Restart to be sure, that clearing stale logs task was ran
    node11.restart_clickhouse(kill=True)

    for i in range(5):
        # check file doesn't exist
        node11.exec_in_container(
            [
                "bash",
                "-c",
                "test ! -e /var/lib/clickhouse/data/default/restore_table/wal_{0}_{0}.bin".format(
                    i
                ),
            ]
        )

    # New wal file was created and ready to write part to it
    # Check file exists
    node11.exec_in_container(
        ["bash", "-c", "test -f /var/lib/clickhouse/data/default/restore_table/wal.bin"]
    )
    # Chech file empty
    node11.exec_in_container(
        [
            "bash",
            "-c",
            "test ! -s /var/lib/clickhouse/data/default/restore_table/wal.bin",
        ]
    )


def test_in_memory_deduplication(start_cluster):
    for i in range(3):
        # table can be in readonly node
        exec_query_with_retry(
            node9,
            "INSERT INTO deduplication_table (date, id, s) VALUES (toDate('2020-03-03'), 1, 'foo')",
        )
        exec_query_with_retry(
            node10,
            "INSERT INTO deduplication_table (date, id, s) VALUES (toDate('2020-03-03'), 1, 'foo')",
        )

    node9.query("SYSTEM SYNC REPLICA deduplication_table", timeout=20)
    node10.query("SYSTEM SYNC REPLICA deduplication_table", timeout=20)

    assert (
        node9.query("SELECT date, id, s FROM deduplication_table")
        == "2020-03-03\t1\tfoo\n"
    )
    assert (
        node10.query("SELECT date, id, s FROM deduplication_table")
        == "2020-03-03\t1\tfoo\n"
    )


# Checks that restoring from WAL works after table schema changed
def test_in_memory_alters(start_cluster):
    def check_parts_type(parts_num):
        assert (
            node9.query(
                "SELECT part_type, count() FROM system.parts WHERE table = 'alters_table' \
             AND active GROUP BY part_type"
            )
            == "InMemory\t{}\n".format(parts_num)
        )

    node9.query(
        "INSERT INTO alters_table (date, id, s) VALUES (toDate('2020-10-10'), 1, 'ab'), (toDate('2020-10-10'), 2, 'cd')"
    )
    node9.query("ALTER TABLE alters_table ADD COLUMN col1 UInt32")
    node9.restart_clickhouse(kill=True)

    expected = "1\tab\t0\n2\tcd\t0\n"
    assert node9.query("SELECT id, s, col1 FROM alters_table ORDER BY id") == expected
    check_parts_type(1)
    # After hard restart table can be in readonly mode
    exec_query_with_retry(
        node9,
        "INSERT INTO alters_table (date, id, col1) VALUES (toDate('2020-10-10'), 3, 100)",
    )
    node9.query("ALTER TABLE alters_table MODIFY COLUMN col1 String")
    node9.query("ALTER TABLE alters_table DROP COLUMN s")
    node9.restart_clickhouse(kill=True)

    check_parts_type(2)
    with pytest.raises(Exception):
        node9.query("SELECT id, s, col1 FROM alters_table")

    # Values of col1 was not materialized as integers, so they have
    # default string values after alter
    expected = "1\t_foo\n2\t_foo\n3\t100_foo\n"
    assert (
        node9.query("SELECT id, col1 || '_foo' FROM alters_table ORDER BY id")
        == expected
    )


def test_polymorphic_parts_index(start_cluster):
    node1.query(
        "CREATE DATABASE test_index ENGINE=Ordinary"
    )  # Different paths with Atomic
    node1.query(
        """
        CREATE TABLE test_index.index_compact(a UInt32, s String)
        ENGINE = MergeTree ORDER BY a
        SETTINGS min_rows_for_wide_part = 1000, index_granularity = 128, merge_max_block_size = 100"""
    )

    node1.query(
        "INSERT INTO test_index.index_compact SELECT number, toString(number) FROM numbers(100)"
    )
    node1.query(
        "INSERT INTO test_index.index_compact SELECT number, toString(number) FROM numbers(30)"
    )
    node1.query("OPTIMIZE TABLE test_index.index_compact FINAL")

    assert (
        node1.query(
            "SELECT part_type FROM system.parts WHERE table = 'index_compact' AND active"
        )
        == "Compact\n"
    )
    assert (
        node1.query(
            "SELECT marks FROM system.parts WHERE table = 'index_compact' AND active"
        )
        == "2\n"
    )

    index_path = os.path.join(
        node1.path, "database/data/test_index/index_compact/all_1_2_1/primary.idx"
    )
    f = open(index_path, "rb")

    assert os.path.getsize(index_path) == 8
    assert struct.unpack("I", f.read(4))[0] == 0
    assert struct.unpack("I", f.read(4))[0] == 99
