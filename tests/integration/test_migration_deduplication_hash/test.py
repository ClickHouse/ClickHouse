import logging
import pytest
from contextlib import contextmanager

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        # A released binary that still understands `insert_deduplication_version`, pinned to
        # `compatible_double_hashes` so it writes both the legacy and the unified deduplication
        # hashes. It stands in for a replica still in the migration's compatible phase during a
        # rolling upgrade to the current unified-only binary.
        cluster.add_instance(
            "node_compatible",
            macros={"replica": "node_compatible"},
            with_installed_binary=True,
            image="clickhouse/clickhouse-server",
            tag="26.5",
            main_configs=[
                "configs/migration_compatible.xml",
            ],
            with_zookeeper=True,
            stay_alive=True,
        )
        # The current build, which always uses the unified deduplication hash.
        cluster.add_instance(
            "node_new",
            macros={"replica": "node_new"},
            with_zookeeper=True,
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
@pytest.mark.parametrize("first", ["compatible", "new"])
def test_cross_version_compatible_to_unified(cluster, insert_type, first):
    # Rolling-upgrade compatibility: a 26.5 replica running `compatible_double_hashes` (which writes
    # both the legacy and the unified deduplication hashes) shares one replicated table with a
    # current replica that uses only the unified hash. The same rows inserted from both replicas must
    # cross-deduplicate via the unified `/deduplication_hashes` directory, leaving a single copy.
    create_table_query = \
"""
    DROP TABLE IF EXISTS test_cross_version_compatible_to_unified;

    CREATE TABLE test_cross_version_compatible_to_unified (key UInt32, value UInt32)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/test_cross_version_compatible_to_unified/', '{replica}')
    PRIMARY KEY key ORDER BY key
"""

    drop_table_query = "DROP TABLE IF EXISTS test_cross_version_compatible_to_unified SYNC"

    node_compatible = cluster.instances["node_compatible"]
    node_new = cluster.instances["node_new"]
    nodes = [node_compatible, node_new]

    with with_tables(
        nodes=nodes,
        create_query=create_table_query,
        drop_query=drop_table_query,
    ):
        async_insert = insert_type == "async"
        insert_settings = f"async_insert={1 if async_insert else 0}, deduplicate_insert='enable'"
        # Cross-version deduplication must be order-independent: whichever replica inserts first
        # publishes the unified hash to keeper, and the other must recognize the same rows as a
        # duplicate. `first` exercises both directions (compatible -> current and current -> compatible).
        first_node, second_node = (node_compatible, node_new) if first == "compatible" else (node_new, node_compatible)
        first_node.query(f"INSERT INTO test_cross_version_compatible_to_unified SETTINGS {insert_settings} VALUES (1, 100), (2, 200), (3, 300)")
        second_node.query(f"INSERT INTO test_cross_version_compatible_to_unified SETTINGS {insert_settings} VALUES (1, 100), (2, 200), (3, 300)")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_cross_version_compatible_to_unified")

        for node in nodes:
            result = node.query("SELECT * FROM test_cross_version_compatible_to_unified ORDER BY key")
            assert result == "1\t100\n2\t200\n3\t300\n"

        for node in nodes:
            node.query("TRUNCATE TABLE test_cross_version_compatible_to_unified")

        for node in nodes:
            node.query("SYSTEM SYNC REPLICA test_cross_version_compatible_to_unified")


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


def test_legacy_async_blocks_cleanup(cluster):
    # During a rolling upgrade an older replica (here the pinned 26.5 in compatible_double_hashes) keeps
    # writing legacy async-insert ids under /async_blocks. The current leader must trim that directory
    # down to replicated_deduplication_window_for_async_inserts; otherwise old replicas would
    # over-deduplicate past the window or leak Keeper nodes. To make the current binary's restored sweep
    # deterministically the cleaner (cleanup runs on the leader, and which replica leads is otherwise
    # racy), the 26.5 replica is stopped after it writes, leaving the current replica as the sole leader.
    # Without the sweep /async_blocks would stay at num_inserts instead of being trimmed to window.
    node_compatible = cluster.instances["node_compatible"]
    node_new = cluster.instances["node_new"]

    zk_path = "/clickhouse/tables/test_legacy_async_blocks_cleanup"
    async_blocks_path = zk_path + "/async_blocks"
    window = 3
    num_inserts = 6

    drop_query = "DROP TABLE IF EXISTS test_legacy_async_blocks_cleanup SYNC"

    try:
        node_compatible.query(
            f"""
            CREATE TABLE test_legacy_async_blocks_cleanup (k UInt32)
            ENGINE=ReplicatedMergeTree('{zk_path}', '{{replica}}')
            ORDER BY k
            """
        )
        # Pin both the floor and the cap of the cleanup schedule low. The /async_blocks sweep is the
        # background cleanup thread, whose adaptive scheduler backs off up to max_cleanup_delay_period
        # (default 300s) when a pass finds nothing to clean. Without the low cap a single early pass
        # (before all the ids are written) reschedules 5 minutes out and never re-runs within the test.
        # max_cleanup_delay_period must be greater than cleanup_delay_period, hence 2.
        node_new.query(
            f"""
            CREATE TABLE test_legacy_async_blocks_cleanup (k UInt32)
            ENGINE=ReplicatedMergeTree('{zk_path}', '{{replica}}')
            ORDER BY k
            SETTINGS replicated_deduplication_window_for_async_inserts = {window},
                     cleanup_delay_period = 1, cleanup_delay_period_random_add = 1,
                     max_cleanup_delay_period = 2
            """
        )

        # The 26.5 replica writes num_inserts distinct async inserts; compatible_double_hashes writes a
        # legacy /async_blocks id for each, so the directory accumulates more entries than the window.
        for i in range(num_inserts):
            node_compatible.query(
                "INSERT INTO test_legacy_async_blocks_cleanup "
                "SETTINGS async_insert = 1, wait_for_async_insert = 1, async_insert_deduplicate = 1 "
                f"VALUES ({i})"
            )

        # Stop the 26.5 replica so the current replica is the sole leader; its restored sweep must then
        # trim /async_blocks down to the window.
        node_compatible.stop_clickhouse()
        assert_eq_with_retry(
            node_new,
            f"SELECT count() FROM system.zookeeper WHERE path = '{async_blocks_path}'",
            str(window),
            retry_count=90,
            sleep_time=1,
        )
    finally:
        node_compatible.start_clickhouse()
        node_compatible.query(drop_query)
        node_new.query(drop_query)
