import time
import uuid

import pytest

import helpers.client as client
from helpers.cluster import CLICKHOUSE_CI_MIN_TESTED_VERSION, ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry, exec_query_with_retry
from helpers.wait_for_helpers import (
    wait_for_delete_empty_parts,
    wait_for_delete_inactive_parts,
)

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance("node1", with_zookeeper=True)
node2 = cluster.add_instance("node2", with_zookeeper=True)

node3 = cluster.add_instance("node3", with_zookeeper=True)
node4 = cluster.add_instance(
    "node4",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
    main_configs=[
        "configs/compat.xml",
    ],
)

node5 = cluster.add_instance(
    "node5",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
    main_configs=[
        "configs/compat.xml",
    ],
)
node6 = cluster.add_instance(
    "node6",
    with_zookeeper=True,
    image="clickhouse/clickhouse-server",
    tag=CLICKHOUSE_CI_MIN_TESTED_VERSION,
    stay_alive=True,
    with_installed_binary=True,
    main_configs=[
        "configs/compat.xml",
    ],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    except Exception as ex:
        print(ex)

    finally:
        cluster.shutdown()


# Column TTL works only with wide parts, because it's very expensive to apply it for compact parts
def test_ttl_columns(started_cluster):
    table_name = f"test_ttl_{node1.name}_{node2.name}"
    for node in [node1, node2]:
        node.query(
            """
                CREATE TABLE {table_name}(date DateTime, id UInt32, a Int32 TTL date + INTERVAL 1 DAY, b Int32 TTL date + INTERVAL 1 MONTH)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl_columns', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date)
                SETTINGS merge_with_ttl_timeout=0, min_bytes_for_wide_part=0, max_merge_selecting_sleep_ms=6000;
            """.format(
                table_name=table_name, replica=node.name
            )
        )

    node1.query(
        f"INSERT INTO {table_name} VALUES (toDateTime('2000-10-10 00:00:00'), 1, 1, 3)"
    )
    node1.query(
        f"INSERT INTO {table_name} VALUES (toDateTime('2000-10-11 10:00:00'), 2, 2, 4)"
    )
    time.sleep(1)  # sleep to allow use ttl merge selector for second time
    node1.query(f"OPTIMIZE TABLE {table_name} FINAL")

    expected = "1\t0\t0\n2\t0\t0\n"
    assert TSV(node1.query(f"SELECT id, a, b FROM {table_name}  ORDER BY id")) == TSV(
        expected
    )
    assert TSV(node2.query(f"SELECT id, a, b FROM {table_name}  ORDER BY id")) == TSV(
        expected
    )

    # Cleanup
    for node in [node1, node2]:
        node.query(f"DROP TABLE {table_name} SYNC")


def test_merge_with_ttl_timeout(started_cluster):
    table = f"test_merge_with_ttl_timeout_{node1.name}_{node2.name}"
    for node in [node1, node2]:
        node.query(
            """
                CREATE TABLE {table}(date DateTime, id UInt32, a Int32 TTL date + INTERVAL 1 DAY, b Int32 TTL date + INTERVAL 1 MONTH)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table}', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date)
                SETTINGS min_bytes_for_wide_part=0, max_merge_selecting_sleep_ms=6000;
            """.format(
                replica=node.name, table=table
            )
        )

    node1.query("SYSTEM STOP TTL MERGES {table}".format(table=table))
    node2.query("SYSTEM STOP TTL MERGES {table}".format(table=table))

    for i in range(1, 4):
        node1.query(
            "INSERT INTO {table} VALUES (toDateTime('2000-10-{day:02d} 10:00:00'), 1, 2, 3)".format(
                day=i, table=table
            )
        )

    assert (
        node1.query("SELECT countIf(a = 0) FROM {table}".format(table=table)) == "0\n"
    )
    assert (
        node2.query("SELECT countIf(a = 0) FROM {table}".format(table=table)) == "0\n"
    )

    node1.query("SYSTEM START TTL MERGES {table}".format(table=table))
    node2.query("SYSTEM START TTL MERGES {table}".format(table=table))

    time.sleep(15)  # TTL merges shall happen.

    for i in range(1, 4):
        node1.query(
            "INSERT INTO {table} VALUES (toDateTime('2000-10-{day:02d} 10:00:00'), 1, 2, 3)".format(
                day=i, table=table
            )
        )

    assert_eq_with_retry(
        node1, "SELECT countIf(a = 0) FROM {table}".format(table=table), "3\n"
    )
    assert_eq_with_retry(
        node2, "SELECT countIf(a = 0) FROM {table}".format(table=table), "3\n"
    )

    # Cleanup
    for node in [node1, node2]:
        node.query(f"DROP TABLE {table} SYNC")


def test_ttl_many_columns(started_cluster):
    table = f"test_ttl_2{node1.name}_{node2.name}"
    for node in [node1, node2]:
        node.query(
            """
                CREATE TABLE {table}(date DateTime, id UInt32,
                    a Int32 TTL date,
                    _idx Int32 TTL date,
                    _offset Int32 TTL date,
                    _partition Int32 TTL date)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl_2', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date) SETTINGS merge_with_ttl_timeout=0, max_merge_selecting_sleep_ms=6000;
            """.format(
                table=table, replica=node.name
            )
        )

    node1.query(f"SYSTEM STOP TTL MERGES {table}")
    node2.query(f"SYSTEM STOP TTL MERGES {table}")

    node1.query(
        f"INSERT INTO {table} VALUES (toDateTime('2000-10-10 00:00:00'), 1, 2, 3, 4, 5)"
    )
    node1.query(
        f"INSERT INTO {table} VALUES (toDateTime('2100-10-10 10:00:00'), 6, 7, 8, 9, 10)"
    )

    node2.query(f"SYSTEM SYNC REPLICA {table}", timeout=5)

    # Check that part will appear in result of merge
    node1.query(f"SYSTEM STOP FETCHES {table}")
    node2.query(f"SYSTEM STOP FETCHES {table}")

    node1.query(f"SYSTEM START TTL MERGES {table}")
    node2.query(f"SYSTEM START TTL MERGES {table}")

    time.sleep(1)  # sleep to allow use ttl merge selector for second time
    node1.query(f"OPTIMIZE TABLE {table} FINAL", timeout=5)

    node2.query(f"SYSTEM SYNC REPLICA {table}", timeout=5)

    expected = "1\t0\t0\t0\t0\n6\t7\t8\t9\t10\n"
    assert TSV(
        node1.query(f"SELECT id, a, _idx, _offset, _partition FROM {table} ORDER BY id")
    ) == TSV(expected)
    assert TSV(
        node2.query(f"SELECT id, a, _idx, _offset, _partition FROM {table} ORDER BY id")
    ) == TSV(expected)

    # Cleanup
    for node in [node1, node2]:
        node.query(f"DROP TABLE {table} SYNC")


@pytest.mark.parametrize(
    "delete_suffix",
    [
        "",
        "DELETE",
    ],
)
def test_ttl_table(started_cluster, delete_suffix):
    table = f"test_ttl_table_{delete_suffix}_{node1.name}_{node2.name}"
    for node in [node1, node2]:
        node.query(
            """
                CREATE TABLE {table}(date DateTime, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table}', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date)
                TTL date + INTERVAL 1 DAY {delete_suffix} SETTINGS merge_with_ttl_timeout=0, max_merge_selecting_sleep_ms=6000;
            """.format(
                table=table, replica=node.name, delete_suffix=delete_suffix
            )
        )

    node1.query(f"INSERT INTO {table} VALUES (toDateTime('2000-10-10 00:00:00'), 1)")
    node1.query(f"INSERT INTO {table} VALUES (toDateTime('2000-10-11 10:00:00'), 2)")
    time.sleep(1)  # sleep to allow use ttl merge selector for second time
    node1.query(f"OPTIMIZE TABLE {table} FINAL")

    assert TSV(node1.query(f"SELECT * FROM {table}")) == TSV("")
    assert TSV(node2.query(f"SELECT * FROM {table}")) == TSV("")

    # Cleanup
    for node in [node1, node2]:
        node.query(f"DROP TABLE {table} SYNC")


def test_modify_ttl(started_cluster):
    table = f"test_modify_ttl_{node1.name}_{node2.name}"
    for node in [node1, node2]:
        node.query(
            """
                CREATE TABLE {table}(d DateTime, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table}', '{replica}')
                ORDER BY id
            """.format(
                table=table, replica=node.name
            )
        )

    node1.query(
        f"INSERT INTO {table} VALUES (now() - INTERVAL 5 HOUR, 1), (now() - INTERVAL 3 HOUR, 2), (now() - INTERVAL 1 HOUR, 3)"
    )
    node2.query(f"SYSTEM SYNC REPLICA {table}", timeout=20)

    node1.query(
        f"ALTER TABLE {table} MODIFY TTL d + INTERVAL 4 HOUR SETTINGS replication_alter_partitions_sync = 2"
    )
    assert node2.query(f"SELECT id FROM {table}") == "2\n3\n"

    node2.query(
        f"ALTER TABLE {table} MODIFY TTL d + INTERVAL 2 HOUR SETTINGS replication_alter_partitions_sync = 2"
    )
    assert node1.query(f"SELECT id FROM {table}") == "3\n"

    node1.query(
        f"ALTER TABLE {table} MODIFY TTL d + INTERVAL 30 MINUTE SETTINGS replication_alter_partitions_sync = 2"
    )
    assert node2.query(f"SELECT id FROM {table}") == ""

    # Cleanup
    for node in [node1, node2]:
        node.query(f"DROP TABLE {table} SYNC")


def test_modify_column_ttl(started_cluster):
    table = f"test_modify_column_ttl_{node1.name}_{node2.name}"
    for node in [node1, node2]:
        node.query(
            """
                CREATE TABLE {table}(d DateTime, id UInt32 DEFAULT 42)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table}', '{replica}')
                ORDER BY d
            """.format(
                table=table, replica=node.name
            )
        )

    node1.query(
        f"INSERT INTO {table} VALUES (now() - INTERVAL 5 HOUR, 1), (now() - INTERVAL 3 HOUR, 2), (now() - INTERVAL 1 HOUR, 3)"
    )
    node2.query(f"SYSTEM SYNC REPLICA {table}", timeout=20)

    node1.query(
        f"ALTER TABLE {table} MODIFY COLUMN id UInt32 TTL d + INTERVAL 4 HOUR SETTINGS replication_alter_partitions_sync = 2"
    )
    assert node2.query(f"SELECT id FROM {table}") == "42\n2\n3\n"

    node1.query(
        f"ALTER TABLE {table} MODIFY COLUMN id UInt32 TTL d + INTERVAL 2 HOUR SETTINGS replication_alter_partitions_sync = 2"
    )
    assert node1.query(f"SELECT id FROM {table}") == "42\n42\n3\n"

    node1.query(
        f"ALTER TABLE {table} MODIFY COLUMN id UInt32 TTL d + INTERVAL 30 MINUTE SETTINGS replication_alter_partitions_sync = 2"
    )
    assert node2.query(f"SELECT id FROM {table}") == "42\n42\n42\n"

    # Cleanup
    for node in [node1, node2]:
        node.query(f"DROP TABLE {table} SYNC")


def test_ttl_double_delete_rule_returns_error(started_cluster):
    table = "test_ttl_double_delete_rule_returns_error"
    try:
        node1.query(
            """
            CREATE TABLE {table}(date DateTime, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table}', '{replica}')
            ORDER BY id PARTITION BY toDayOfMonth(date)
            TTL date + INTERVAL 1 DAY, date + INTERVAL 2 DAY SETTINGS merge_with_ttl_timeout=0, max_merge_selecting_sleep_ms=6000
        """.format(
                table=table, replica=node1.name
            )
        )
        assert False
    except client.QueryRuntimeException:
        pass
    except:
        assert False


def optimize_with_retry(node, table_name, retry=20):
    for i in range(retry):
        try:
            node.query(
                "OPTIMIZE TABLE {name} FINAL SETTINGS optimize_throw_if_noop = 1".format(
                    name=table_name
                ),
                settings={"optimize_throw_if_noop": "1"},
            )
            break
        except:
            time.sleep(0.5)


@pytest.mark.parametrize(
    "name,engine",
    [
        pytest.param(
            "test_ttl_alter_delete", "MergeTree()", id="test_ttl_alter_delete"
        ),
        pytest.param(
            "test_replicated_ttl_alter_delete",
            "ReplicatedMergeTree('/clickhouse/test_replicated_ttl_alter_delete', '1')",
            id="test_ttl_alter_delete_replicated",
        ),
    ],
)
def test_ttl_alter_delete(started_cluster, name, engine):
    """Check compatibility with old TTL delete expressions to make sure
    that:
    * alter modify of column's TTL delete expression works
    * alter to add new columns works
    * alter modify to add TTL delete expression to a a new column works
    for a table that has TTL delete expression defined but
    no explicit storage policy assigned.
    """

    node1.query(
        """
            CREATE TABLE {name} (
                s1 String,
                d1 DateTime
            ) ENGINE = {engine}
            ORDER BY tuple()
            TTL d1 + INTERVAL 1 DAY DELETE
            SETTINGS min_bytes_for_wide_part=0
        """.format(
            name=name, engine=engine
        )
    )

    node1.query(
        """ALTER TABLE {name} MODIFY COLUMN s1 String TTL d1 + INTERVAL 1 SECOND""".format(
            name=name
        )
    )
    node1.query("""ALTER TABLE {name} ADD COLUMN b1 Int32""".format(name=name))

    node1.query(
        """INSERT INTO {name} (s1, b1, d1) VALUES ('hello1', 1, toDateTime({time}))""".format(
            name=name, time=time.time()
        )
    )
    node1.query(
        """INSERT INTO {name} (s1, b1, d1) VALUES ('hello2', 2, toDateTime({time}))""".format(
            name=name, time=time.time() + 360
        )
    )

    time.sleep(1)

    optimize_with_retry(node1, name)
    r = node1.query(
        "SELECT s1, b1 FROM {name} ORDER BY b1, s1".format(name=name)
    ).splitlines()
    assert r == ["\t1", "hello2\t2"]

    node1.query(
        """ALTER TABLE {name} MODIFY COLUMN b1 Int32 TTL d1""".format(name=name)
    )
    node1.query(
        """INSERT INTO {name} (s1, b1, d1) VALUES ('hello3', 3, toDateTime({time}))""".format(
            name=name, time=time.time()
        )
    )

    time.sleep(1)

    optimize_with_retry(node1, name)

    r = node1.query(
        "SELECT s1, b1 FROM {name} ORDER BY b1, s1".format(name=name)
    ).splitlines()
    assert r == ["\t0", "\t0", "hello2\t2"]

    # Cleanup
    node1.query(f"DROP TABLE {name} SYNC")


def test_ttl_empty_parts(started_cluster):
    for node in [node1, node2]:
        node.query(
            """
            CREATE TABLE test_ttl_empty_parts(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/test_ttl_empty_parts', '{replica}')
            ORDER BY id
            SETTINGS max_bytes_to_merge_at_min_space_in_pool = 1, max_bytes_to_merge_at_max_space_in_pool = 1,
                cleanup_delay_period = 1, cleanup_delay_period_random_add = 0,
                cleanup_thread_preferred_points_per_iteration=0, old_parts_lifetime = 1

        """.format(
                replica=node.name
            )
        )

    for i in range(1, 7):
        node1.query(
            "INSERT INTO test_ttl_empty_parts SELECT '2{}00-01-0{}', number FROM numbers(1000)".format(
                i % 2, i
            )
        )

    assert node1.query("SELECT count() FROM test_ttl_empty_parts") == "6000\n"
    assert (
        node1.query(
            "SELECT name FROM system.parts WHERE table = 'test_ttl_empty_parts' AND active ORDER BY name"
        )
        == "all_0_0_0\nall_1_1_0\nall_2_2_0\nall_3_3_0\nall_4_4_0\nall_5_5_0\n"
    )

    node1.query("ALTER TABLE test_ttl_empty_parts MODIFY TTL date")

    assert node1.query("SELECT count() FROM test_ttl_empty_parts") == "3000\n"

    # Wait for cleanup thread
    wait_for_delete_empty_parts(node1, "test_ttl_empty_parts")
    wait_for_delete_inactive_parts(node1, "test_ttl_empty_parts")

    assert (
        node1.query(
            "SELECT name FROM system.parts WHERE table = 'test_ttl_empty_parts' AND active ORDER BY name"
        )
        == "all_0_0_0_6\nall_2_2_0_6\nall_4_4_0_6\n"
    )

    for node in [node1, node2]:
        node.query(
            "ALTER TABLE test_ttl_empty_parts MODIFY SETTING max_bytes_to_merge_at_min_space_in_pool = 1000000000"
        )
        node.query(
            "ALTER TABLE test_ttl_empty_parts MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1000000000"
        )

    optimize_with_retry(node1, "test_ttl_empty_parts")
    assert (
        node1.query(
            "SELECT name FROM system.parts WHERE table = 'test_ttl_empty_parts' AND active ORDER BY name"
        )
        == "all_0_4_1_6\n"
    )

    # Check that after removing empty parts mutations and merges works
    node1.query(
        "INSERT INTO test_ttl_empty_parts SELECT '2100-01-20', number FROM numbers(1000)"
    )
    node1.query(
        "ALTER TABLE test_ttl_empty_parts DELETE WHERE id % 2 = 0 SETTINGS mutations_sync = 2"
    )
    assert node1.query("SELECT count() FROM test_ttl_empty_parts") == "2000\n"

    optimize_with_retry(node1, "test_ttl_empty_parts")
    assert (
        node1.query(
            "SELECT name FROM system.parts WHERE table = 'test_ttl_empty_parts' AND active ORDER BY name"
        )
        == "all_0_7_2_8\n"
    )

    node2.query("SYSTEM SYNC REPLICA test_ttl_empty_parts", timeout=20)

    error_msg = (
        "<Error> default.test_ttl_empty_parts (ReplicatedMergeTreeCleanupThread)"
    )
    assert not node1.contains_in_log(error_msg)
    assert not node2.contains_in_log(error_msg)

    # Cleanup
    for node in [node1, node2]:
        node.query("DROP TABLE test_ttl_empty_parts SYNC")


@pytest.mark.parametrize(
    ("node_left", "node_right", "num_run"),
    [(node1, node2, 0), (node3, node4, 1), (node5, node6, 2)],
)
def test_ttl_compatibility(started_cluster, node_left, node_right, num_run):
    if node_left.is_built_with_memory_sanitizer():
        pytest.skip(
            "Memory Sanitizer is too slow for this timing-sensitive test"
        )

    # The test times out for sanitizer/ARM builds, so we increase the timeout.
    timeout = 60
    if node_left.is_built_with_sanitizer() or node_right.is_built_with_sanitizer() or \
    node_left.is_built_with_llvm_coverage() or node_right.is_built_with_llvm_coverage():
        timeout = 300

    table = f"test_ttl_compatibility_{node_left.name}_{node_right.name}_{num_run}"
    for node in [node_left, node_right]:
        node.query(
            """
                DROP TABLE IF EXISTS {table}_delete SYNC;
                CREATE TABLE {table}_delete(date DateTime, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table}_delete', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date)
                TTL date + INTERVAL 3 SECOND;
            """.format(
                table=table, replica=node.name
            )
        )

        node.query(
            """
                DROP TABLE IF EXISTS {table}_group_by SYNC;
                CREATE TABLE {table}_group_by(date DateTime, id UInt32, val UInt64)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table}_group_by', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date)
                TTL date + INTERVAL 3 SECOND GROUP BY id SET val = sum(val);
            """.format(
                table=table, replica=node.name
            )
        )

        node.query(
            """
                DROP TABLE IF EXISTS {table}_where SYNC;
                CREATE TABLE {table}_where(date DateTime, id UInt32)
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{table}_where', '{replica}')
                ORDER BY id PARTITION BY toDayOfMonth(date)
                TTL date + INTERVAL 3 SECOND DELETE WHERE id % 2 = 1;
            """.format(
                table=table, replica=node.name
            )
        )

    # Compute a fixed timestamp once so that all rows use the same toDayOfMonth
    # and land in the same partition, even if the test runs across midnight.
    # Using a literal from now() also ensures the data is NOT yet expired at
    # insert time, so the old binary doesn't start premature TTL merges.
    now_str = node_left.query("SELECT toString(now())").strip()
    expired = f"toDateTime('{now_str}')"

    node_left.query(f"INSERT INTO {table}_delete VALUES ({expired}, 1)")
    node_left.query(
        f"INSERT INTO {table}_delete VALUES (toDateTime('2100-10-11 10:00:00'), 2)"
    )
    node_right.query(f"INSERT INTO {table}_delete VALUES ({expired}, 3)")
    node_right.query(
        f"INSERT INTO {table}_delete VALUES (toDateTime('2100-10-11 10:00:00'), 4)"
    )

    node_left.query(f"INSERT INTO {table}_group_by VALUES ({expired}, 0, 1)")
    node_left.query(f"INSERT INTO {table}_group_by VALUES ({expired}, 0, 2)")
    node_right.query(f"INSERT INTO {table}_group_by VALUES ({expired}, 0, 3)")
    node_right.query(f"INSERT INTO {table}_group_by VALUES ({expired}, 0, 4)")

    node_left.query(f"INSERT INTO {table}_where VALUES ({expired}, 1)")
    node_left.query(f"INSERT INTO {table}_where VALUES ({expired}, 2)")
    node_right.query(f"INSERT INTO {table}_where VALUES ({expired}, 3)")
    node_right.query(f"INSERT INTO {table}_where VALUES ({expired}, 4)")

    if node_left.with_installed_binary:
        node_left.restart_with_latest_version()

    if node_right.with_installed_binary:
        node_right.restart_with_latest_version()

    time.sleep(5)  # Wait for TTL

    # Disable TTL merge cooldown so that OPTIMIZE TABLE FINAL can re-trigger
    # TTL merges immediately if the first merge was only partial.
    # We set this after restart (not in CREATE TABLE) to avoid an infinite
    # TTL rewrite loop on the old binary that creates thousands of outdated parts.
    for suffix in ["_delete", "_group_by", "_where"]:
        for node in [node_left, node_right]:
            exec_query_with_retry(
                node,
                f"ALTER TABLE {table}{suffix} MODIFY SETTING merge_with_ttl_timeout=0",
            )

    # Wait for all TTL merges to complete on both replicas in a single shared
    # loop.  Previous versions used 6 sequential assert_eq_with_retry calls,
    # each with an independent timeout equal to `timeout` seconds.  Under
    # sanitizers, where merges are slow, the cumulative wait could exceed the
    # test-framework's 900 s limit.
    #
    # This loop shares a single time budget and periodically re-triggers
    # OPTIMIZE TABLE FINAL + SYSTEM SYNC REPLICA to nudge stalled TTL merges
    # (which can get stuck behind the concurrent-TTL-merge limit).
    expectations = [
        (node_left,  f"SELECT id FROM {table}_delete ORDER BY id",   "2\n4\n"),
        (node_right, f"SELECT id FROM {table}_delete ORDER BY id",   "2\n4\n"),
        (node_left,  f"SELECT val FROM {table}_group_by ORDER BY id", "10\n"),
        (node_right, f"SELECT val FROM {table}_group_by ORDER BY id", "10\n"),
        (node_left,  f"SELECT id FROM {table}_where ORDER BY id",    "2\n4\n"),
        (node_right, f"SELECT id FROM {table}_where ORDER BY id",    "2\n4\n"),
    ]

    deadline = time.monotonic() + timeout
    optimize_interval = 10  # re-trigger OPTIMIZE every N seconds
    last_optimize = 0

    while time.monotonic() < deadline:
        now = time.monotonic()

        # Periodically re-trigger OPTIMIZE FINAL and SYNC REPLICA to push
        # stalled merges forward.
        if now - last_optimize >= optimize_interval:
            last_optimize = now
            for suffix in ["_delete", "_group_by", "_where"]:
                for node in [node_left, node_right]:
                    try:
                        node.query(f"OPTIMIZE TABLE {table}{suffix} FINAL", timeout=30)
                    except Exception:
                        pass
                    try:
                        node.query(f"SYSTEM SYNC REPLICA {table}{suffix}", timeout=30)
                    except Exception:
                        pass

        # Check all expectations.
        all_ok = True
        last_mismatch = None
        for node, query, expected in expectations:
            try:
                result = node.query(query)
                if TSV(result) != TSV(expected):
                    all_ok = False
                    last_mismatch = (node.name, query, expected.strip(), result.strip())
            except Exception:
                all_ok = False

        if all_ok:
            break

        time.sleep(1)
    else:
        if last_mismatch:
            node_name, query, expected, got = last_mismatch
            raise AssertionError(
                f"Timeout waiting for TTL results on {node_name}.\n"
                f"  Query:    {query}\n"
                f"  Expected: {expected}\n"
                f"  Got:      {got}"
            )
        raise AssertionError("Timeout waiting for TTL results (all queries failed)")

    # Cleanup
    for node in [node_left, node_right]:
        node.query(f"DROP TABLE {table}_delete SYNC")
        node.query(f"DROP TABLE {table}_group_by SYNC")
        node.query(f"DROP TABLE {table}_where SYNC")


def test_ttl_drop_parts_limit(started_cluster):
    table = f"test_merges_mutations_limit_{uuid.uuid4().hex}"

    max_parts_to_merge_at_once = 123
    node1.query(
        f"""
        CREATE TABLE {table} (
            date DateTime,
            id UInt32,
            value String
        )
        ENGINE = MergeTree()
        ORDER BY id
        TTL date + INTERVAL 1 DAY
        SETTINGS merge_with_ttl_timeout=0, max_merge_selecting_sleep_ms=6000,
        -- Disables ordinary merges, but not TTL merges
        min_parts_to_merge_at_once=2000,
        -- Sets limit for TTL merges
        max_parts_to_merge_at_once = {max_parts_to_merge_at_once}
        """
    )

    # Stop merges, to be able to accumulate a big number of parts
    node1.query(f"SYSTEM STOP MERGES {table}")

    # Insert many parts (over 1000) with old dates that should expire
    parts_count = 1100
    old_date = "toDateTime('2000-01-01 00:00:00')"

    for i in range(parts_count):
        node1.query(
            f"INSERT INTO {table} VALUES ({old_date}, {i}, 'value_{i}')"
        )

    # Verify we have many parts
    initial_parts = int(node1.query(
        f"SELECT count() FROM system.parts WHERE table = '{table}' AND active"
    ).strip())
    assert initial_parts >= parts_count, f"Expected at least {parts_count} parts, got {initial_parts}"

    # Verify data exists before TTL merge
    initial_rows = int(node1.query(f"SELECT count() FROM {table}").strip())
    assert initial_rows == parts_count, f"Expected {parts_count} rows, got {initial_rows}"

    node1.query(f"SYSTEM START MERGES {table}")

    # Wait for TTL merges to process the parts and remove data
    max_wait_iterations = 100
    final_rows = initial_rows
    for _ in range(max_wait_iterations):
        final_rows = int(node1.query(f"SELECT count() FROM {table}").strip())
        if final_rows == 0:
            break
        time.sleep(0.5)

    # Verify that TTL has removed the data
    assert final_rows == 0, f"Expected 0 rows after TTL expiration, got {final_rows}"

    # Verify parts were removed
    final_parts = int(node1.query(
        f"SELECT count() FROM system.parts WHERE table = '{table}' AND active AND rows > 0"
    ).strip())
    assert final_parts == 0, f"Expected zero parts after merge, got {final_parts} (was {initial_parts})"

    node1.query("SYSTEM FLUSH LOGS")

    # Check that no merge created with more than max_parts_to_merge_at_once parts
    max_parts_in_merge = int(node1.query(
        f"""
        SELECT max(length(merged_from))
        FROM system.part_log
        WHERE table = '{table}' AND merge_reason = 'TTLDropMerge'
        """
    ))

    assert max_parts_in_merge == max_parts_to_merge_at_once, f"Found merge with {max_parts_in_merge} parts, which exceeds max_parts_to_merge_at_once limit of {max_parts_to_merge_at_once}"

    node1.query(f"DROP TABLE {table} SYNC")
