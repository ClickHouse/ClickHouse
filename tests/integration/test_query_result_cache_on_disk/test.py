#!/usr/bin/env python3
# Tests that the query result cache on-disk tier persists results across a server restart
# (write-through to disk on the first run, served from disk after restart). This also guards
# against regressions in the on-disk serialization format, in particular multi-line query
# strings and the encoding of the `is_subquery` dimension in the entry file name.

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/query_cache.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# A deliberately multi-line query: the query string contains newlines, which would desynchronize
# a newline-delimited on-disk format and cause the entry to be silently dropped after restart.
MULTILINE_QUERY = """SELECT
    number,
    number * 2
FROM numbers(10)
ORDER BY number"""


def cache_on_disk_entries():
    return int(
        node.query(
            "SELECT value FROM system.metrics WHERE metric = 'QueryCacheOnDiskEntries'"
        ).strip()
    )


def run_cached(query, query_id, extra_settings=None):
    settings = {"use_query_cache": 1}
    if extra_settings:
        settings.update(extra_settings)
    return node.query(
        query,
        query_id=query_id,
        settings=settings,
    )


def cache_hits_for(query_id):
    node.query("SYSTEM FLUSH LOGS")
    return int(
        node.query(
            f"""
            SELECT ProfileEvents['QueryCacheHits']
            FROM system.query_log
            WHERE query_id = '{query_id}' AND type = 'QueryFinish'
            ORDER BY event_time_microseconds DESC
            LIMIT 1
            """
        ).strip()
    )


def test_query_result_cache_survives_restart():
    node.query("SYSTEM DROP QUERY CACHE")

    # First execution: cache miss, result is computed and written through to disk.
    expected = run_cached(MULTILINE_QUERY, "qrc_disk_first")
    assert expected != ""
    assert cache_on_disk_entries() >= 1

    # Restart wipes the in-memory cache; the on-disk tier must repopulate it.
    node.restart_clickhouse()

    # The entry is loaded from disk at startup, so it must be visible before any query runs.
    assert cache_on_disk_entries() >= 1

    # Second execution after restart: must be served from the cache (and therefore from disk,
    # since the in-memory cache was cleared by the restart). Result must match exactly.
    result = run_cached(MULTILINE_QUERY, "qrc_disk_after_restart")
    assert result == expected
    assert cache_hits_for("qrc_disk_after_restart") == 1


# A JOIN that replicates a single left row across several matching right rows. With
# `enable_lazy_columns_replication` (on by default) the left string column is materialized
# lazily as a `ColumnReplicated`. When such a result is compressed and written to the on-disk
# tier, the replicated column is hidden inside a compressed column; `NativeWriter` de-replicates
# before it decompresses, so the `ColumnReplicated` reconstructed by decompression used to reach
# the serialization unchanged and trigger a "Bad cast" logical error. The replicated column must
# be materialized before compression so the result is persisted and served after a restart.
#
# `arrayJoin` is the vehicle that fans out the right side into several rows with the same join
# key; it is classified as a non-deterministic function, so caching it requires
# `query_cache_nondeterministic_function_handling = 'save'`. The result is content-deterministic
# (the array is a constant), so the exact-match assertion across the restart still holds.
REPLICATED_COLUMN_QUERY = """SELECT l.s
FROM (SELECT materialize('replicated value') AS s, 1 AS k) AS l
INNER JOIN (SELECT arrayJoin([1, 1, 1]) AS k) AS r ON l.k = r.k
ORDER BY l.s"""

REPLICATED_COLUMN_SETTINGS = {"query_cache_nondeterministic_function_handling": "save"}


def test_query_result_cache_on_disk_replicated_columns():
    node.query("SYSTEM DROP QUERY CACHE")

    # First execution: the result contains a replicated column and is written through to disk.
    expected = run_cached(
        REPLICATED_COLUMN_QUERY,
        "qrc_disk_replicated_first",
        extra_settings=REPLICATED_COLUMN_SETTINGS,
    )
    assert expected != ""
    assert cache_on_disk_entries() >= 1

    # Restart wipes the in-memory cache; the on-disk tier must repopulate it.
    node.restart_clickhouse()
    assert cache_on_disk_entries() >= 1

    # Served from disk after the restart; the materialized result must match exactly.
    result = run_cached(
        REPLICATED_COLUMN_QUERY,
        "qrc_disk_replicated_after_restart",
        extra_settings=REPLICATED_COLUMN_SETTINGS,
    )
    assert result == expected
    assert cache_hits_for("qrc_disk_replicated_after_restart") == 1


STALE_REFRESH_QUERY = "SELECT number FROM numbers(7) ORDER BY number"


def test_query_result_cache_on_disk_refreshes_stale_entry():
    # The on-disk policy is a plain LRU and is not TTL-aware. A persisted entry that has expired is
    # correctly rejected by the reader, but the writer must still be able to overwrite it with a fresh
    # result. Otherwise the stale metadata makes the writer skip the insert ("already in disk cache"),
    # so the query result on disk can never be refreshed until size/count eviction or SYSTEM DROP QUERY
    # CACHE. Restarts isolate the disk tier: each restart wipes the in-memory cache, so a cache hit can
    # only be served from disk.
    node.query("SYSTEM DROP QUERY CACHE")

    # First execution with a short TTL: cache miss, written through to disk and about to expire.
    expected = run_cached(
        STALE_REFRESH_QUERY,
        "qrc_disk_stale_first",
        extra_settings={"query_cache_ttl": 1},
    )
    assert expected != ""
    assert cache_on_disk_entries() >= 1

    # Let the persisted entry expire, then restart so only the (now stale) on-disk entry remains.
    time.sleep(3)
    node.restart_clickhouse()
    assert cache_on_disk_entries() >= 1

    # Re-execute with a long TTL: the reader rejects the stale on-disk entry (miss) and the writer must
    # replace it on disk with a fresh, long-lived result instead of skipping because an entry exists.
    result = run_cached(
        STALE_REFRESH_QUERY,
        "qrc_disk_stale_refresh",
        extra_settings={"query_cache_ttl": 60},
    )
    assert result == expected
    assert cache_hits_for("qrc_disk_stale_refresh") == 0

    # Restart again to clear the in-memory cache, then the refreshed on-disk entry must serve a hit.
    node.restart_clickhouse()
    result = run_cached(
        STALE_REFRESH_QUERY,
        "qrc_disk_stale_after_refresh",
        extra_settings={"query_cache_ttl": 60},
    )
    assert result == expected
    assert cache_hits_for("qrc_disk_stale_after_refresh") == 1
