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
        # No skip indexes: the data and index uncompressed caches share the same
        # `UncompressedCache*` profile events, so only the data cache must be
        # able to produce events here.
        node.query("""
            CREATE TABLE t (
                id UInt64,
                s String
            )
            ENGINE = MergeTree
            ORDER BY id
            SETTINGS index_granularity = 1024
            """)
        node.query("INSERT INTO t SELECT number, toString(number) FROM numbers(200000)")
        yield
    finally:
        cluster.shutdown()


def run_eligible_query(query_id):
    # `use_uncompressed_cache` is left unset, so only the automatic decision
    # (`enable_automatic_use_uncompressed_cache`) controls cache usage. The
    # setting is passed in the query text because that is the only client path
    # that distinguishes an explicitly set value from an unset one.
    node.query(
        "SELECT sum(length(s)) FROM t WHERE id < 50000 FORMAT Null "
        "SETTINGS enable_automatic_use_uncompressed_cache = 1",
        query_id=query_id,
    )


def get_cache_events(query_id):
    node.query("SYSTEM FLUSH LOGS")
    row = node.query(f"""
        SELECT
            ProfileEvents['UncompressedCacheHits'],
            ProfileEvents['UncompressedCacheMisses']
        FROM system.query_log
        WHERE query_id = '{query_id}' AND type = 'QueryFinish'
        FORMAT TSV
        """).strip()
    assert row, f"No QueryFinish entry in system.query_log for query_id={query_id}"
    hits, misses = row.split("\t")
    return int(hits), int(misses)


def test_auto_uncompressed_cache_reload_to_zero():
    # Phase 1: cache size is positive, the automatic decision enables the cache
    # for the eligible local read.
    node.query("SYSTEM DROP UNCOMPRESSED CACHE")
    run_eligible_query("reload_phase1")
    hits, misses = get_cache_events("reload_phase1")
    assert misses > 0, (
        f"Expected misses from the automatically enabled cache, "
        f"got hits={hits}, misses={misses}"
    )

    # Phase 2: resize the cache to zero via runtime SYSTEM RELOAD CONFIG. The
    # automatic decision must observe the live cache size, not the value of the
    # `uncompressed_cache_size` server setting captured at startup, and must not
    # route reads through the zero-sized cache (that would record misses and
    # immediately evict every entry).
    with node.with_replace_config(
        CACHE_CONFIG_PATH,
        "<clickhouse><uncompressed_cache_size>0</uncompressed_cache_size></clickhouse>",
        reload_before=True,
        reload_after=True,
    ):
        run_eligible_query("reload_phase2")
        hits, misses = get_cache_events("reload_phase2")
        assert (hits, misses) == (0, 0), (
            f"Expected zero uncompressed-cache events after the cache was "
            f"resized to zero at runtime, got hits={hits}, misses={misses}"
        )

    # Phase 3: original config restored and reloaded by `reload_after=True`.
    # The automatic decision must use the cache again.
    node.query("SYSTEM DROP UNCOMPRESSED CACHE")
    run_eligible_query("reload_phase3")
    hits, misses = get_cache_events("reload_phase3")
    assert misses > 0, (
        f"Expected misses after the cache was re-enabled at runtime, "
        f"got hits={hits}, misses={misses}"
    )
