import logging
import os
import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/split_cache.xml", "configs/config.d/test_logger.xml"],
    stay_alive=True,
    with_minio=True,
)


def wait_for_cache_initialized(node, cache_name, max_attempts=50):
    initialized = False
    attempts = 0
    while not initialized:
        initialized = bool(
            node.query(
                f"SELECT is_initialized FROM system.filesystem_cache_settings WHERE is_initialized and cache_name='{cache_name}'"
            )
        )

        if initialized:
            break

        time.sleep(0.1)
        attempts += 1
        if attempts >= max_attempts:
            raise Exception("Stopped waiting for cache to be initialized")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_split_cache_silly_test(started_cluster):
    node.query("DROP TABLE IF EXISTS t0")
    node.query(
        """CREATE TABLE t0 (
            key UInt64,
            value UInt64
        )
        ENGINE = MergeTree
        PRIMARY KEY key
        SETTINGS storage_policy = 'split_cache_slru'
        """
    )
    node.query("INSERT INTO t0 SELECT rand()%1000, rand()%1000 FROM numbers(10000)")
    node.query("SELECT * FROM t0;")

    node.query("DROP TABLE t0")


def test_split_cache_restart(started_cluster):
    node.query("DROP TABLE IF EXISTS t0")
    node.query(
        """CREATE TABLE t0 (
            key UInt64,
            value UInt64
        )
        ENGINE = MergeTree
        PRIMARY KEY key
        SETTINGS storage_policy = 'split_cache_slru'
        """
    )
    node.query("INSERT INTO t0 SELECT rand()%1000, rand()%1000 FROM numbers(10000)")
    node.query("OPTIMIZE TABLE t0")
    node.query("SYSTEM STOP MERGES t0")

    node.query("SYSTEM CLEAR FILESYSTEM CACHE 'split_cache_slru'")
    node.restart_clickhouse()
    wait_for_cache_initialized(node, "split_cache_slru")

    cache_state = node.query(
        "SELECT key, file_segment_range_begin, size FROM system.filesystem_cache WHERE size > 0 ORDER BY key, file_segment_range_begin, size"
    )
    cache_count = int(
        node.query("SELECT count() FROM system.filesystem_cache WHERE cache_name = 'split_cache_slru' AND size > 0")
    )

    node.restart_clickhouse()
    wait_for_cache_initialized(node, "split_cache_slru")

    cache_state_after_restart = node.query(
        "SELECT key, file_segment_range_begin, size FROM system.filesystem_cache WHERE size > 0 ORDER BY key, file_segment_range_begin, size"
    )
    new_cache_count = int(
        node.query("SELECT count() FROM system.filesystem_cache WHERE cache_name = 'split_cache_slru' AND size > 0")
    )

    print(f"Cache state before restart:\n{cache_state}")
    print(f"Cache state after restart:\n{cache_state_after_restart}")

    # Background operations (outdated parts loading, background downloads, cleanup)
    # may change cache state slightly between restarts, so use tolerance.
    if cache_count > 0:
        fraction = abs(new_cache_count - cache_count) / cache_count
        assert fraction <= 0.5, f"Cache count changed too much: {cache_count} -> {new_cache_count}"

    node.query("DROP TABLE t0")


@pytest.mark.parametrize("storage_policy", [("split_cache_slru"), ("split_cache_lru")])
def test_split_cache_system_files_no_eviction(started_cluster, storage_policy):
    """
    Note: Check that after full scan of the table files that are needed for restart will be still presented in cache.
    Total size of system fiels in this case is about 350 KiB; data files is 17 MiB;
    Size of system segment of cache is 1 MiB; Data segment of cache is 4 MiB.

    WITH parts AS
        (
            SELECT replaceAll(path, system.disks.path, '')
            FROM system.parts
            LEFT JOIN system.disks ON system.parts.disk_name = system.disks.name
            WHERE active AND (`table` = 't0')
        )
    SELECT
        splitByChar('.', local_path)[-1] AS ext,
        formatReadableSize(sum(size))
    FROM system.remote_data_paths
    WHERE substring(local_path, 1, (length(local_path) - position(reverse(local_path), '/')) + 1) IN (parts)
    GROUP BY ext

       ┌─ext───┬─formatReadab⋯(sum(size))─┐
    1. │ bin   │ 16.67 MiB                │
    2. │ cmrk2 │ 126.19 KiB               │
    3. │ txt   │ 95.70 KiB                │
    4. │ json  │ 32.83 KiB                │
    5. │ cidx  │ 47.27 KiB                │
       └───────┴──────────────────────────┘
    """
    # Generaly they should be different, but for simplicity they are equal.
    filesystem_cache_name = storage_policy

    node.query("DROP TABLE IF EXISTS t0")
    node.query(
        f"""CREATE TABLE t0 (
            key UInt64,
            value UInt64
        )
        ENGINE = MergeTree
        PRIMARY KEY key
        SETTINGS storage_policy = '{storage_policy}',
        min_bytes_for_wide_part = 0
        """
    )

    for _ in range(100):
        node.query(
            """
                INSERT INTO t0 SELECT rand()%1000, rand()%1000 FROM numbers(1000000)
                """
        )

    node.query("SYSTEM STOP MERGES t0")
    node.query(f"SYSTEM CLEAR FILESYSTEM CACHE '{filesystem_cache_name}'")
    node.restart_clickhouse()
    wait_for_cache_initialized(node, storage_policy)

    count = int(
        node.query(
            f"SELECT count(*) FROM system.filesystem_cache WHERE cache_name = '{filesystem_cache_name}' AND segment_type='System'"
        )
    )
    assert count > 0

    def assert_cache_state():
        """
        The state of cache can change with background processes, so make sure that the state does not change dramatically.
        """
        current_count = int(
            node.query(
                f"SELECT count(*) FROM system.filesystem_cache WHERE cache_name = '{filesystem_cache_name}' AND segment_type='System'"
            )
        )
        fraction = abs(current_count - count) / count
        assert fraction <= 0.5, f"System cache count changed too much: {count} -> {current_count}"

    node.query("SELECT * FROM t0 FORMAT NULL")

    assert_cache_state()

    node.restart_clickhouse()
    wait_for_cache_initialized(node, storage_policy)
    assert_cache_state()

    node.query("DROP TABLE t0 SYNC")
