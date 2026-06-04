#!/usr/bin/env python3
# Tests that the query result cache on-disk tier persists results across a server restart
# (write-through to disk on the first run, served from disk after restart). This also guards
# against regressions in the on-disk serialization format, in particular multi-line query
# strings and the encoding of the `is_subquery` dimension in the entry file name.

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


def run_cached(query, query_id):
    return node.query(
        query,
        query_id=query_id,
        settings={"use_query_cache": 1},
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
