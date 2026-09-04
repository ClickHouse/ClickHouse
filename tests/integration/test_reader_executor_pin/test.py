"""In-flight segment protection of the `ReaderExecutor` (`use_reader_executor`)
against `SYSTEM DROP FILESYSTEM CACHE`.

While a sequential scan is paused mid-window with a partially-filled FileCache
segment still held by the plan's writer, a concurrent cache drop must NOT evict
that segment: a held segment is non-releasable (`use_count > 1`), so the drop
(`removeAllReleasable`) skips it. Once the scan finishes and the plan releases
it, a drop clears the cache, and the scan returns correct results despite the
mid-scan drop.

The pause is injected via the `reader_executor_pause_after_window` failpoint,
which fires only when the fetch frontier lands strictly inside a partially filled
cache segment (`frontierInPartial`), which requires (a) prefetch machines to run at all - the executor
suppresses them under high memory pressure, which is why this scenario lives in
an integration test on a dedicated node rather than a stateless test on a shared
pressured server - and (b) geometry where a fetch cut lands mid-segment:
5 MiB segments (config) against the 16 MiB fill-ahead horizon and the 32 MiB
plan window (query setting) - neither a multiple of the segment size, so the
lead cut and the plan-boundary job end both land inside a segment - with one
reader (`max_threads=1`) holding one big task extent and a file long enough
that the lead outlives the segment-ceiled fetch allowance (see the INSERT).
"""

import threading

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/config.d/storage_conf.xml"],
    with_minio=True,
    stay_alive=True,
)

FP = "reader_executor_pause_after_window"

SCAN = "SELECT count(), sum(cityHash64(value)) FROM t_re_pin"
SCAN_SETTINGS = {
    "use_reader_executor": 1,
    "max_threads": 1,
    "reader_executor_plan_look_ahead_max_window": 33554432,
}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def cached_segments():
    return int(node.query("SELECT count() FROM system.filesystem_cache").strip())


def test_pin_survives_cache_drop(started_cluster):
    node.query("DROP TABLE IF EXISTS t_re_pin")
    node.query(
        """
        CREATE TABLE t_re_pin (key UInt32, value String)
        ENGINE = MergeTree() ORDER BY key
        SETTINGS storage_policy = 's3_cache', min_bytes_for_wide_part = 0
        """
    )
    # Incompressible column data, ~72 MiB on disk: several 5 MiB cache segments.
    # The fetch allowance (consumed reach, ceiled to whole segments) trails at
    # ~2x the consumed bytes, so the 16 MiB lead only becomes the binding cut -
    # the one cut that can end a fetch mid-segment, which the pin needs - once
    # ~16 MiB are consumed. The file must run well past that crossover; ~30 MiB
    # was enough only while the reach formula over-weighted history.
    node.query(
        "INSERT INTO t_re_pin SELECT number, randomPrintableASCII(100) FROM numbers(750000)"
    )
    expected = node.query(SCAN)

    # The pause fires when a read-ahead machine is collected with a partial
    # in-flight segment pinned. On slow (sanitizer) builds the consumer can win
    # the race and interrupt every machine below the cursor - those collects
    # never pin - so retry the scan until one machine completes ahead. Each
    # attempt re-colds the cache; every scan, paused or not, must be correct.
    paused = False
    result = {}
    scanner = None
    for _ in range(5):
        node.query("SYSTEM DROP FILESYSTEM CACHE")
        node.query(f"SYSTEM ENABLE FAILPOINT {FP}")
        result = {}
        scanner = threading.Thread(
            target=lambda: result.update(got=node.query(SCAN, settings=SCAN_SETTINGS))
        )
        scanner.start()
        try:
            # Blocks until the scan is paused at the failpoint with the pin held.
            node.query(f"SYSTEM WAIT FAILPOINT {FP} PAUSE", timeout=30)
            paused = True
            break
        except Exception:
            node.query(f"SYSTEM DISABLE FAILPOINT {FP}")
            scanner.join(timeout=120)
            assert not scanner.is_alive(), "the unpaused scan must finish"
            assert result["got"] == expected, "the unpaused scan must be correct"
    assert paused, "the scan never paused at the pin failpoint in 5 attempts"

    try:
        # While paused the in-flight segment is pinned: the drop must leave it.
        node.query("SYSTEM DROP FILESYSTEM CACHE")
        assert cached_segments() > 0, "the pinned segment must survive the mid-scan drop"
    finally:
        node.query(f"SYSTEM DISABLE FAILPOINT {FP}")
        scanner.join(timeout=120)

    assert not scanner.is_alive(), "the scan must finish once the failpoint is released"
    assert result["got"] == expected, "the scan must be correct despite the mid-scan drop"

    # The pin is released with the scan, but server-side teardown (executor
    # destruction, prefetch-worker drain) can lag the client's return; retry the
    # drop until the cache empties - a genuine pin leak never empties it.
    for _ in range(100):
        node.query("SYSTEM DROP FILESYSTEM CACHE")
        if cached_segments() == 0:
            break
    assert cached_segments() == 0, "no segment may stay pinned after the scan"

    node.query("DROP TABLE t_re_pin")
