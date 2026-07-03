import logging
import pytest
from contextlib import contextmanager

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "node_2512",
            macros={"replica": "node_2512"},
            with_installed_binary=True,
            image="clickhouse/clickhouse-server",
            tag="25.12",
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_old",
            macros={"replica": "node_old"},
            main_configs=[
                "configs/migration_old.xml",
            ],
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_compatible",
            macros={"replica": "node_compatible"},
            main_configs=[
                "configs/migration_compatible.xml",
            ],
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_new",
            macros={"replica": "node_new"},
            main_configs=[
                "configs/migration_new.xml",
            ],
            with_zookeeper=True,
        )
        cluster.add_instance(
            "node_restart",
            macros={"replica": "node_restart"},
            main_configs=[
                "configs/migration_restart.xml",
            ],
            with_zookeeper=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


@contextmanager
def with_tables(nodes, create_query, drop_query):
    try:
        for node in nodes:
            node.query(create_query)

        yield
    finally:
        for node in nodes:
            node.query(drop_query)


@pytest.mark.parametrize("insert_type", ["sync", "async"])
def test_replicated_table_migration(cluster, insert_type):
    create_table_query = \
"""
    DROP TABLE IF EXISTS test_replicated_table_migration;

    CREATE TABLE test_replicated_table_migration (key UInt32, value UInt32)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_replicated_table_migration/', '{replica}')
    PRIMARY KEY key ORDER BY key
"""

    drop_table_query = "DROP TABLE IF EXISTS test_replicated_table_migration SYNC"

    node_old = cluster.instances["node_old"]
    node_compatible = cluster.instances["node_compatible"]
    node_new = cluster.instances["node_new"]

    all_nodes = [node_old, node_compatible, node_new]

    def check_insert_deduplicated_across_nodes(nodes, insert_type):
        for node in nodes:
            async_insert = insert_type == "async"
            node.query(f"INSERT INTO test_replicated_table_migration SETTINGS async_insert={1 if async_insert else 0}, deduplicate_insert='enable' VALUES (1, 100), (2, 200), (3, 300)")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_replicated_table_migration")

        for node in nodes:
            result = node.query("SELECT * FROM test_replicated_table_migration ORDER BY key")
            assert result == "1\t100\n2\t200\n3\t300\n"

        for node in nodes:
            node.query("TRUNCATE TABLE test_replicated_table_migration")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_replicated_table_migration")

    with with_tables(
        nodes=all_nodes,
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        check_insert_deduplicated_across_nodes([node_old, node_compatible], insert_type)
        check_insert_deduplicated_across_nodes([node_compatible, node_old], insert_type)

        check_insert_deduplicated_across_nodes([node_compatible, node_new], insert_type)
        check_insert_deduplicated_across_nodes([node_new, node_compatible], insert_type)



def test_not_replicated_table_migration(cluster):
    create_table_query = \
"""
    DROP TABLE IF EXISTS test_not_replicated_table_migration;

    CREATE TABLE test_not_replicated_table_migration (key UInt32, value UInt32)
    ENGINE=MergeTree
    PRIMARY KEY key ORDER BY key
    SETTINGS non_replicated_deduplication_window = 10000
"""

    drop_table_query = "DROP TABLE IF EXISTS test_not_replicated_table_migration SYNC"

    config_old = \
"""
<clickhouse>
    <insert_deduplication_version>old_separate_hashes</insert_deduplication_version>
</clickhouse>
"""

    config_compatible = \
"""
<clickhouse>
    <insert_deduplication_version>compatible_double_hashes</insert_deduplication_version>
</clickhouse>
"""

    config_new = \
"""
<clickhouse>
    <insert_deduplication_version>new_unified_hash</insert_deduplication_version>
</clickhouse>
"""

    def check_insert_deduplicated_across_configs(node, first_config, second_config):
        node.replace_config("/etc/clickhouse-server/config.d/migration_restart.xml", first_config)
        node.restart_clickhouse()

        node.query("INSERT INTO test_not_replicated_table_migration VALUES (1, 100), (2, 200), (3, 300)")

        node.replace_config("/etc/clickhouse-server/config.d/migration_restart.xml", second_config)
        node.restart_clickhouse()

        node.query("INSERT INTO test_not_replicated_table_migration VALUES (1, 100), (2, 200), (3, 300)")

        result = node.query("SELECT * FROM test_not_replicated_table_migration ORDER BY key")
        assert result == "1\t100\n2\t200\n3\t300\n"

        node.query("TRUNCATE TABLE test_not_replicated_table_migration")

    node = cluster.instances["node_restart"]

    with with_tables(
        nodes=[node],
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        check_insert_deduplicated_across_configs(node, config_old, config_compatible)
        check_insert_deduplicated_across_configs(node, config_compatible, config_new)


@pytest.mark.parametrize("first_insert_type", ["sync", "async"])
@pytest.mark.parametrize("second_insert_type", ["sync", "async"])
@pytest.mark.parametrize("first_node", ["node_compatible", "node_new"])
@pytest.mark.parametrize("second_node", ["node_compatible", "node_new"])
def test_sync_async_deduplicated(cluster, first_insert_type, second_insert_type, first_node, second_node):
    create_table_query = \
"""
    DROP TABLE IF EXISTS test_sync_async_deduplicated;

    CREATE TABLE test_sync_async_deduplicated (key UInt32, value UInt32)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_sync_async_deduplicated/', '{replica}')
    PRIMARY KEY key ORDER BY key
"""

    drop_table_query = "DROP TABLE IF EXISTS test_sync_async_deduplicated SYNC"

    def check_insert_deduplicated_across_nodes(node1, node2, type_first, type_second):
        async_type_first = type_first == "async"
        node1.query(f"INSERT INTO test_sync_async_deduplicated SETTINGS async_insert={1 if async_type_first else 0}, deduplicate_insert='enable' VALUES (1, 100), (2, 200), (3, 300)")
        async_type_second = type_second == "async"
        node2.query(f"INSERT INTO test_sync_async_deduplicated SETTINGS async_insert={1 if async_type_second else 0}, deduplicate_insert='enable' VALUES (1, 100), (2, 200), (3, 300)")

        nodes = [node1, node2]

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_sync_async_deduplicated")

        for node in nodes:
            result = node.query("SELECT * FROM test_sync_async_deduplicated ORDER BY key")
            assert result == "1\t100\n2\t200\n3\t300\n"

        for node in nodes:
            node.query("TRUNCATE TABLE test_sync_async_deduplicated")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_sync_async_deduplicated")

    node1 = cluster.instances[first_node]
    node2 = cluster.instances[second_node]

    with with_tables(
        nodes=[node1] if (first_node == second_node) else [node1, node2],
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        check_insert_deduplicated_across_nodes(node1, node2, first_insert_type, second_insert_type)


@pytest.mark.parametrize("insert_type", ["sync", "async"])
def test_sync_async_deduplicated_with_2512(cluster, insert_type):
    create_table_query = \
"""
    DROP TABLE IF EXISTS test_sync_async_deduplicated_with_2512;

    CREATE TABLE test_sync_async_deduplicated_with_2512 (key UInt32, value UInt32)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_sync_async_deduplicated_with_2512/', '{replica}')
    PRIMARY KEY key ORDER BY key
"""

    drop_table_query = "DROP TABLE IF EXISTS test_sync_async_deduplicated_with_2512 SYNC"

    node_2512 = cluster.instances["node_2512"]
    node_compatible = cluster.instances["node_compatible"]
    nodes = [node_2512, node_compatible]

    with with_tables(
        nodes=nodes,
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        async_insert = insert_type == "async"
        node_2512.query(f"INSERT INTO test_sync_async_deduplicated_with_2512 SETTINGS async_insert={1 if async_insert else 0}, insert_deduplicate=1, async_insert_deduplicate=1 VALUES (1, 100), (2, 200), (3, 300)")
        node_compatible.query(f"INSERT INTO test_sync_async_deduplicated_with_2512 SETTINGS async_insert={1 if async_insert else 0}, deduplicate_insert='enable' VALUES (1, 100), (2, 200), (3, 300)")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_sync_async_deduplicated_with_2512")

        for node in nodes:
            result = node.query("SELECT * FROM test_sync_async_deduplicated_with_2512 ORDER BY key")
            assert result == "1\t100\n2\t200\n3\t300\n"

        for node in nodes:
            node.query("TRUNCATE TABLE test_sync_async_deduplicated_with_2512")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_sync_async_deduplicated_with_2512")


def test_new_unified_hash_self_deduplication_variable_length(cluster):
    # Regression test for a deduplication hash bug with new_unified_hash.
    # ColumnString/ColumnArray::updateHashWithValueRange hashed absolute offsets, so equal rows
    # located at different offsets within one async-insert flush produced different unified
    # deduplication ids and were not self-deduplicated. Fixed-width columns (e.g. UInt32) are
    # position-invariant and did not expose the bug, hence the String/Array columns here.
    node_new = cluster.instances["node_new"]

    create_table_query = \
"""
    DROP TABLE IF EXISTS test_unified_self_dedup;

    CREATE TABLE test_unified_self_dedup (key UInt32, s String, a Array(UInt32))
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_unified_self_dedup/', '{replica}')
    ORDER BY key
"""

    drop_table_query = "DROP TABLE IF EXISTS test_unified_self_dedup SYNC"

    # Keep the busy timeout high and do not wait for the flush, so both inserts accumulate in the
    # queue and are combined into a single flush (one block with two offsets) by SYSTEM FLUSH.
    async_settings = (
        "async_insert=1, wait_for_async_insert=0, async_insert_use_adaptive_busy_timeout=0, "
        "async_insert_busy_timeout_min_ms=10000, async_insert_busy_timeout_max_ms=50000, "
        "deduplicate_insert='enable'"
    )

    with with_tables(
        nodes=[node_new],
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        node_new.query(f"INSERT INTO test_unified_self_dedup SETTINGS {async_settings} VALUES (1, 'one line', [10, 20])")
        node_new.query(f"INSERT INTO test_unified_self_dedup SETTINGS {async_settings} VALUES (1, 'one line', [10, 20])")
        node_new.query("SYSTEM FLUSH ASYNC INSERT QUEUE")

        # Two identical inserts combined in one flush must self-deduplicate to a single row.
        result = node_new.query("SELECT key, s, a FROM test_unified_self_dedup ORDER BY key")
        assert result == "1\tone line\t[10,20]\n"


def test_cleanup_consistent_across_directories_compatible(cluster):
    # Under `compatible_double_hashes` a sync insert writes its deduplication id to BOTH the legacy
    # `blocks` directory (old per-part hash) and the unified `deduplication_hashes` directory. The
    # cleanup thread must evict from both consistently under `replicated_deduplication_window`.
    # Functional test 04005 covers the same invariant for the default `new_unified_hash`, which uses
    # only `deduplication_hashes`; this pins the dual-directory mode, selectable only via a server
    # setting, so the cross-directory cleanup stays regression-tested after the default flip.
    node = cluster.instances["node_compatible"]

    create_table_query = \
"""
    DROP TABLE IF EXISTS dedup_cleanup_compatible SYNC;

    CREATE TABLE dedup_cleanup_compatible (date Date, id UInt32, value String)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/dedup_cleanup_compatible', '{replica}')
    PARTITION BY date ORDER BY id
    SETTINGS replicated_deduplication_window = 2, cleanup_delay_period = 1,
             cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0
"""

    drop_table_query = "DROP TABLE IF EXISTS dedup_cleanup_compatible SYNC"

    with with_tables(
        nodes=[node],
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        zk_path = node.query(
            "SELECT zookeeper_path FROM system.replicas "
            "WHERE database = currentDatabase() AND table = 'dedup_cleanup_compatible'"
        ).strip().rstrip("/")

        def dir_count_query(directory):
            return f"SELECT count() FROM system.zookeeper WHERE path = '{zk_path}/{directory}'"

        def wait_both_dirs_cleaned_to(expected):
            # The legacy `blocks` and unified `deduplication_hashes` directories share the sync
            # window, so both must settle to the same count.
            assert_eq_with_retry(node, dir_count_query("blocks"), str(expected), retry_count=60, sleep_time=1)
            assert_eq_with_retry(node, dir_count_query("deduplication_hashes"), str(expected), retry_count=60, sleep_time=1)

        # Three distinct sync inserts (async_insert = 0, so ids land in the SYNC `blocks` directory
        # rather than `async_blocks`); each also lands in the unified `deduplication_hashes`.
        node.query("INSERT INTO dedup_cleanup_compatible SETTINGS async_insert = 0 VALUES (toDate('2024-01-01'), 1, 'a')")
        node.query("INSERT INTO dedup_cleanup_compatible SETTINGS async_insert = 0 VALUES (toDate('2024-01-01'), 2, 'b')")
        node.query("INSERT INTO dedup_cleanup_compatible SETTINGS async_insert = 0 VALUES (toDate('2024-01-01'), 3, 'c')")
        assert node.query("SELECT count() FROM dedup_cleanup_compatible").strip() == "3"

        # Window is 2, so both directories must be trimmed to 2 entries together.
        wait_both_dirs_cleaned_to(2)

        # The first row's ids were evicted from both directories, so re-inserting it is not deduplicated.
        node.query("INSERT INTO dedup_cleanup_compatible SETTINGS async_insert = 0 VALUES (toDate('2024-01-01'), 1, 'a')")
        assert node.query("SELECT count() FROM dedup_cleanup_compatible").strip() == "4"
        wait_both_dirs_cleaned_to(2)

        node.query("INSERT INTO dedup_cleanup_compatible SETTINGS async_insert = 0 VALUES (toDate('2024-01-01'), 2, 'b')")
        assert node.query("SELECT count() FROM dedup_cleanup_compatible").strip() == "5"
        wait_both_dirs_cleaned_to(2)

        # A retry of the most recent insert, still inside the window, is deduplicated.
        node.query("INSERT INTO dedup_cleanup_compatible SETTINGS async_insert = 0 VALUES (toDate('2024-01-01'), 2, 'b')")
        assert node.query("SELECT count() FROM dedup_cleanup_compatible").strip() == "5"


def test_new_unified_async_enable_follows_sync_window(cluster):
    # Under new_unified_hash the async-insert enable gate follows the unified window
    # (replicated_deduplication_window), not the legacy replicated_deduplication_window_for_async_inserts.
    # So _for_async_inserts = 0 no longer disables async deduplication, and replicated_deduplication_window = 0 does.
    node = cluster.instances["node_new"]

    def two_identical_async_inserts(table):
        for _ in range(2):
            node.query(
                f"INSERT INTO {table} "
                f"SETTINGS async_insert = 1, wait_for_async_insert = 1, deduplicate_insert = 'enable' VALUES (1)"
            )

    # Legacy async window is 0 but the unified window is non-zero: async dedup stays ENABLED, so the
    # two identical inserts collapse to one row.
    create_legacy_zero = \
"""
    DROP TABLE IF EXISTS t_async_enable_legacy_zero SYNC;

    CREATE TABLE t_async_enable_legacy_zero (key UInt32)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/t_async_enable_legacy_zero', '{replica}')
    ORDER BY key
    SETTINGS replicated_deduplication_window_for_async_inserts = 0
"""
    with with_tables(
        nodes=[node],
        create_query=create_legacy_zero,
        drop_query="DROP TABLE IF EXISTS t_async_enable_legacy_zero SYNC",
    ):
        two_identical_async_inserts("t_async_enable_legacy_zero")
        assert node.query("SELECT count() FROM t_async_enable_legacy_zero").strip() == "1"

    # Unified window is 0: async dedup is DISABLED, so both identical inserts land.
    create_unified_zero = \
"""
    DROP TABLE IF EXISTS t_async_enable_unified_zero SYNC;

    CREATE TABLE t_async_enable_unified_zero (key UInt32)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/t_async_enable_unified_zero', '{replica}')
    ORDER BY key
    SETTINGS replicated_deduplication_window = 0
"""
    with with_tables(
        nodes=[node],
        create_query=create_unified_zero,
        drop_query="DROP TABLE IF EXISTS t_async_enable_unified_zero SYNC",
    ):
        two_identical_async_inserts("t_async_enable_unified_zero")
        assert node.query("SELECT count() FROM t_async_enable_unified_zero").strip() == "2"


def test_alias_async_insert_uses_async_window_compatible(cluster):
    # Functional test 04204 (Part 2) checks that an async insert routed through an Alias engine uses
    # the ASYNC deduplication window like a direct insert. It distinguishes the async path from the
    # sync path by disabling the sync window (replicated_deduplication_window = 0) and enabling only
    # the async one (replicated_deduplication_window_for_async_inserts = 10000) - a distinction that
    # exists only while the legacy per-source async window is honored. Under the default new_unified_hash
    # both paths share the sync window, so 04204 can no longer tell whether the Alias sink kept
    # async_insert = true. This pins the scenario to compatible_double_hashes (node_compatible), where
    # the async window is still used and the regression - the Alias sink built as a sync insert - is
    # observable again.
    node = cluster.instances["node_compatible"]

    create_tables = \
"""
    DROP TABLE IF EXISTS alias_async_direct SYNC;
    DROP TABLE IF EXISTS alias_async_target SYNC;
    DROP TABLE IF EXISTS alias_async_alias SYNC;

    CREATE TABLE alias_async_direct (a UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/alias_async_direct', '{replica}')
    ORDER BY a
    SETTINGS replicated_deduplication_window = 0, replicated_deduplication_window_for_async_inserts = 10000;

    CREATE TABLE alias_async_target (a UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/alias_async_target', '{replica}')
    ORDER BY a
    SETTINGS replicated_deduplication_window = 0, replicated_deduplication_window_for_async_inserts = 10000;

    CREATE TABLE alias_async_alias ENGINE = Alias(currentDatabase(), 'alias_async_target')
"""

    drop_tables = \
"""
    DROP TABLE IF EXISTS alias_async_alias SYNC;
    DROP TABLE IF EXISTS alias_async_target SYNC;
    DROP TABLE IF EXISTS alias_async_direct SYNC
"""

    def two_identical_async_batches(table):
        for _ in range(2):
            node.query(
                f"INSERT INTO {table} "
                f"SETTINGS async_insert = 1, wait_for_async_insert = 1, deduplicate_insert = 'enable' VALUES (1), (2), (3)"
            )

    with with_tables(
        nodes=[node],
        create_query=create_tables,
        drop_query=drop_tables,
    ):
        two_identical_async_batches("alias_async_direct")
        two_identical_async_batches("alias_async_alias")

        # The async window deduplicates the repeated batch for a direct insert (3 rows). The Alias must
        # behave identically; the unfixed Alias sink ran with async_insert = false, took the sync window
        # (= 0, no dedup) and kept both batches (6 rows).
        assert node.query("SELECT count() FROM alias_async_direct").strip() == "3"
        assert node.query("SELECT count() FROM alias_async_alias").strip() == "3"
