import pytest

from helpers.cluster import ClickHouseCluster

CACHE_CONFIG_PATH = "/etc/clickhouse-server/config.d/cache_size.xml"

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/cache_size.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        node.query(
            """
            CREATE TABLE t (
                id UInt64,
                s String,
                INDEX bf s TYPE bloom_filter GRANULARITY 1
            )
            ENGINE = MergeTree
            ORDER BY id
            SETTINGS index_granularity = 1024
            """
        )
        # A single part keeps the bloom-filter file deterministic across phases.
        node.query(
            "INSERT INTO t SELECT number, toString(number) FROM numbers(200000)"
        )
        yield
    finally:
        cluster.shutdown()


def run_skip_index_query(query_id):
    # Filtering on `s` forces the bloom_filter skip index to be loaded for every
    # mark range, which is the path that exhibited the bug.
    node.query(
        "SELECT count() FROM t WHERE s = '12345'",
        query_id=query_id,
        settings={"use_skip_indexes": 1},
    )


def get_cache_events(query_id):
    node.query("SYSTEM FLUSH LOGS")
    row = node.query(
        f"""
        SELECT
            ProfileEvents['UncompressedCacheHits'],
            ProfileEvents['UncompressedCacheMisses'],
            ProfileEvents['UncompressedCacheWeightLost']
        FROM system.query_log
        WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        FORMAT TSV
        """
    ).strip()
    assert row, f"No QueryFinish entry in system.query_log for query_id={query_id}"
    hits, misses, weight_lost = row.split("\t")
    return int(hits), int(misses), int(weight_lost)


def test_index_uncompressed_cache_runtime_toggle():
    # Phase 1: cache disabled (size=0 from initial config). The skip-index read
    # path must bypass the cache entirely - no profile events should fire.
    run_skip_index_query("phase1_disabled")
    hits, misses, weight_lost = get_cache_events("phase1_disabled")
    assert (hits, misses, weight_lost) == (0, 0, 0), (
        f"Expected zero index-uncompressed-cache events when the cache is "
        f"disabled, got hits={hits}, misses={misses}, weight_lost={weight_lost}"
    )

    # Phase 2: enable the cache via runtime SYSTEM RELOAD CONFIG, verify that
    # the cache is now actually used (misses on first query, hits on second).
    with node.with_replace_config(
        CACHE_CONFIG_PATH,
        "<clickhouse><index_uncompressed_cache_size>10485760</index_uncompressed_cache_size></clickhouse>",
        reload_before=True,
        reload_after=True,
    ):
        node.query("SYSTEM DROP INDEX UNCOMPRESSED CACHE")

        run_skip_index_query("phase2_first")
        hits, misses, weight_lost = get_cache_events("phase2_first")
        assert misses > 0, (
            f"Expected misses with the cache enabled, "
            f"got hits={hits}, misses={misses}, weight_lost={weight_lost}"
        )
        assert hits == 0, (
            f"Expected no hits on the first query after dropping the cache, "
            f"got hits={hits}"
        )

        run_skip_index_query("phase2_second")
        hits, misses, weight_lost = get_cache_events("phase2_second")
        assert hits > 0, (
            f"Expected hits on the repeated query, "
            f"got hits={hits}, misses={misses}, weight_lost={weight_lost}"
        )

    # Phase 3: original config restored and reloaded by `reload_after=True`.
    # Disabling the cache at runtime must again bypass it for new queries.
    run_skip_index_query("phase3_redisabled")
    hits, misses, weight_lost = get_cache_events("phase3_redisabled")
    assert (hits, misses, weight_lost) == (0, 0, 0), (
        f"Expected zero index-uncompressed-cache events after re-disabling at "
        f"runtime, got hits={hits}, misses={misses}, weight_lost={weight_lost}"
    )
