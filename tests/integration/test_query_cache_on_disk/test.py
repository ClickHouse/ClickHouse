"""The entries of the query cache on disk (setting `query_cache_on_disk_cache_name`) survive a restart of the server.

They are ordinary entries of the underlying filesystem cache, whose metadata is loaded back from disk on startup, so a result computed
before the restart is served from disk after it, while the in-memory query cache of the restarted server starts empty.

The stateless test `05019_query_cache_on_disk_local` checks the same with separate `clickhouse-local` processes; this test checks the
server, both with a graceful restart and with a killed server (whose entries were fully written when the queries completed), and both
with a plain and with a split (`use_split_cache`) filesystem cache as the backend.
"""

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["config.d/query_cache_on_disk.xml"],
    stay_alive=True,
)

CACHES = ["cache_for_query_results", "split_cache_for_query_results"]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_event(name):
    return int(
        node.query(
            f"SELECT sum(value) FROM system.events WHERE event = '{name}'"
        ).strip()
        or 0
    )


def in_memory_entries(tag):
    return int(
        node.query(
            f"SELECT count() FROM system.query_cache WHERE tag = '{tag}'"
        ).strip()
    )


def run_queries(cache, tag):
    """Runs a deterministic query (with totals and extremes, to cover the extra blocks of an entry) and a non-deterministic one
    (stored explicitly, so that serving it from the cache is observable in the result itself). Returns both results.
    """
    settings = f"use_query_cache = 1, query_cache_on_disk_cache_name = '{cache}', query_cache_tag = '{tag}'"
    deterministic = node.query(
        "SELECT number % 3 AS k, sum(number), max(toString(number)), [toLowCardinality('x'), NULL] "
        "FROM numbers(10000) GROUP BY k WITH TOTALS ORDER BY k "
        f"SETTINGS {settings}, extremes = 1"
    )
    non_deterministic = node.query(
        f"SELECT rand64() SETTINGS {settings}, query_cache_nondeterministic_function_handling = 'save'"
    )
    return deterministic, non_deterministic


def check_survives_restart(cache, kill):
    tag = f"{cache}_{'kill' if kill else 'graceful'}"

    # Before the restart: the results are computed, written to disk and to memory, and nothing is served from disk yet.
    hits_before = get_event("QueryCacheOnDiskHits")
    misses_before = get_event("QueryCacheOnDiskMisses")
    written_before = get_event("QueryCacheOnDiskWrittenBytes")

    results = run_queries(cache, tag)

    assert get_event("QueryCacheOnDiskHits") == hits_before
    assert get_event("QueryCacheOnDiskMisses") == misses_before + 2
    assert get_event("QueryCacheOnDiskWrittenBytes") > written_before
    assert in_memory_entries(tag) == 2

    node.restart_clickhouse(kill=kill)

    # After the restart: the in-memory query cache is empty and the profile events start from zero, so the repeated queries are
    # served from disk (and not from memory), with identical results, and nothing is written again.
    assert in_memory_entries(tag) == 0

    assert run_queries(cache, tag) == results

    assert get_event("QueryCacheOnDiskHits") == 2
    assert get_event("QueryCacheOnDiskMisses") == 0
    assert get_event("QueryCacheOnDiskReadBytes") > 0
    assert get_event("QueryCacheOnDiskWrittenBytes") == 0
    # `QueryCacheHits` counts the query cache as a whole: a miss in memory followed by a hit on disk is one hit.
    assert get_event("QueryCacheHits") == 2
    assert get_event("QueryCacheMisses") == 0

    # A result served from disk is not put into the in-memory cache either.
    assert in_memory_entries(tag) == 0

    # Once more, to check that serving from disk left the entries intact.
    assert run_queries(cache, tag) == results
    assert get_event("QueryCacheOnDiskHits") == 4


@pytest.mark.parametrize("cache", CACHES)
def test_entries_survive_graceful_restart(started_cluster, cache):
    check_survives_restart(cache, kill=False)


@pytest.mark.parametrize("cache", CACHES)
def test_entries_survive_restart_after_kill(started_cluster, cache):
    check_survives_restart(cache, kill=True)
