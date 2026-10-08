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

    def system_segments():
        """
        Identify each cached System segment, not just how many there are: a count can stay the
        same (or grow) while the original segments are evicted and replaced by different ones.
        """
        return set(
            node.query(
                f"SELECT key, file_segment_range_begin, size FROM system.filesystem_cache "
                f"WHERE cache_name = '{filesystem_cache_name}' AND segment_type = 'System' AND size > 0"
            )
            .strip()
            .splitlines()
        )

    def wait_for_stable_system_segments(required_stable_seconds=3.0, timeout_seconds=30.0):
        """
        `wait_for_cache_initialized` only reports that the cache became usable; background
        loading keeps adding System segments afterwards, sometimes in batches with a gap
        between them, so a single repeated sample can land in such a gap and look settled
        right before the next batch arrives. Require the set to stay unchanged for a
        continuous window, not just across one poll, before accepting it as the baseline;
        otherwise the assertions below would compare against a still-partial snapshot,
        defeating this wait entirely. Failing to reach that window within the timeout is
        itself a test failure, not a signal to fall back to whatever was last observed.
        """
        deadline = time.monotonic() + timeout_seconds
        current = system_segments()
        stable_since = time.monotonic()
        while time.monotonic() < deadline:
            time.sleep(0.5)
            sample = system_segments()
            if sample != current:
                current = sample
                stable_since = time.monotonic()
                continue
            if current and time.monotonic() - stable_since >= required_stable_seconds:
                return current
        raise Exception(
            f"System segment set for cache '{filesystem_cache_name}' did not stay "
            f"unchanged for {required_stable_seconds}s within {timeout_seconds}s"
        )

    baseline = wait_for_stable_system_segments()
    assert len(baseline) > 0

    def assert_no_eviction(current):
        """
        System files live in their own cache partition, so the full scan (17 MiB of data through
        a separate 4 MiB data partition) must not push them out, and they must survive a restart.
        Startup and the scan may cache more of them, hence a subset check rather than equality:
        what must not happen is one of the baseline segments disappearing.
        """
        evicted = baseline - current
        assert not evicted, (
            f"{len(evicted)} of {len(baseline)} System segments were evicted, e.g. {sorted(evicted)[:5]}"
        )

    node.query("SELECT * FROM t0 FORMAT NULL")

    assert_no_eviction(system_segments())

    node.restart_clickhouse()
    wait_for_cache_initialized(node, storage_policy)
    # Same race as the baseline: a cache that is still reloading looks like eviction, so only
    # compare once the set has settled.
    assert_no_eviction(wait_for_stable_system_segments())

    node.query("DROP TABLE t0 SYNC")


def test_split_cache_mark_files_in_system_segment(started_cluster):
    """
    Verify that mark files (.cmrk2, .mrk2, etc.) are classified as System
    cache segments when their extensions are listed in `system_cache_extensions`.
    The `split_cache_marks` disk extends the default system extensions with all
    known mark file suffixes.
    """
    filesystem_cache_name = "split_cache_marks"
    mark_extensions = (".cmrk2", ".cmrk3", ".mrk2", ".mrk3", ".cmrk", ".mrk")

    node.query("DROP TABLE IF EXISTS t_marks")
    node.query(
        f"""
        CREATE TABLE t_marks (
            key UInt64,
            value UInt64
        )
        ENGINE = MergeTree
        PRIMARY KEY key
        SETTINGS
            storage_policy = '{filesystem_cache_name}',
            min_bytes_for_wide_part = 0
        """
    )

    for _ in range(10):
        node.query(
            "INSERT INTO t_marks SELECT rand()%1000, rand()%1000 FROM numbers(100000)"
        )

    node.query("SYSTEM STOP MERGES t_marks")
    node.query(f"SYSTEM CLEAR FILESYSTEM CACHE '{filesystem_cache_name}'")

    # Warm the cache via a full scan.
    node.query("SELECT * FROM t_marks FORMAT NULL")

    mark_ext_condition = " OR ".join(
        f"endsWith(rdp.local_path, '{ext}')" for ext in mark_extensions
    )

    system_mark_count = int(
        node.query(
            f"""
            SELECT count()
            FROM system.remote_data_paths AS rdp
            INNER JOIN system.filesystem_cache AS fc
                ON arrayJoin(rdp.cache_paths) = fc.cache_path
            WHERE fc.cache_name = '{filesystem_cache_name}'
              AND fc.segment_type = 'System'
              AND ({mark_ext_condition})
              AND fc.size > 0
            """
        )
    )

    data_mark_count = int(
        node.query(
            f"""
            SELECT count()
            FROM system.remote_data_paths AS rdp
            INNER JOIN system.filesystem_cache AS fc
                ON arrayJoin(rdp.cache_paths) = fc.cache_path
            WHERE fc.cache_name = '{filesystem_cache_name}'
              AND fc.segment_type = 'Data'
              AND ({mark_ext_condition})
              AND fc.size > 0
            """
        )
    )

    assert system_mark_count > 0, (
        "Expected at least one mark-file cache segment classified as System, "
        f"but got {system_mark_count}"
    )
    assert data_mark_count == 0, (
        "Mark files must not appear in the Data cache segment, "
        f"but found {data_mark_count}"
    )

    node.query("DROP TABLE t_marks SYNC")
