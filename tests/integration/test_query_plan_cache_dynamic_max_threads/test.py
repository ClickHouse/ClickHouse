import uuid

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/server.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)

QUERY = """
    SELECT
        a,
        b,
        sum(v) AS total
    FROM query_plan_cache_dynamic_threads
    GROUP BY a, b
    ORDER BY a, b
    SETTINGS
        allow_experimental_query_plan_cache = 1,
        enable_query_plan_cache = {enable_cache},
        max_threads = 4,
        max_threads_min_free_memory_per_thread = {min_free_per_thread}
"""


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def query_with_profile_events(enable_cache, min_free_per_thread):
    query_id = uuid.uuid4().hex
    result = node.query(
        QUERY.format(
            enable_cache=int(enable_cache),
            min_free_per_thread=min_free_per_thread,
        ),
        query_id=query_id,
    )
    node.query("SYSTEM FLUSH LOGS query_log")
    events = node.query(
        f"""
        SELECT
            ProfileEvents['QueryPlanCacheHits'],
            ProfileEvents['QueryPlanCacheMisses'],
            ProfileEvents['QueryPlanCachePreAnalysisHits'],
            ProfileEvents['QueryPlanCacheValidationMisses']
        FROM system.query_log
        WHERE query_id = '{query_id}'
          AND current_database = currentDatabase()
          AND type = 'QueryFinish'
        """
    ).strip()
    return result, tuple(map(int, events.split("\t")))


def allocate_memory_for_single_effective_thread(min_free_per_thread):
    tracked = int(
        node.query(
            "SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'"
        )
    )
    hard_limit = int(
        node.query(
            "SELECT value FROM system.server_settings "
            "WHERE name = 'max_server_memory_usage'"
        )
    )
    # Keep enough headroom for the query itself while still making
    # floor(free_memory / min_free_per_thread) equal to one.
    target_free_memory = min_free_per_thread * 3 // 2
    bytes_to_allocate = hard_limit - tracked - target_free_memory
    assert bytes_to_allocate > 0
    node.query(f"SYSTEM ALLOCATE MEMORY {bytes_to_allocate}")


def test_cached_plan_across_dynamic_max_threads(started_cluster):
    node.query("DROP TABLE IF EXISTS query_plan_cache_dynamic_threads")
    node.query(
        """
        CREATE TABLE query_plan_cache_dynamic_threads
        (a UInt8, b UInt8, c UInt32, v UInt64)
        ENGINE = MergeTree
        ORDER BY (a, b, c)
        """
    )
    node.query("SYSTEM STOP MERGES query_plan_cache_dynamic_threads")
    for part in range(8):
        node.query(
            "INSERT INTO query_plan_cache_dynamic_threads "
            f"SELECT number % 4, number % 8, number + {part * 1000}, number + 1 "
            "FROM numbers(1000)"
        )

    hard_limit = int(
        node.query(
            "SELECT value FROM system.server_settings "
            "WHERE name = 'max_server_memory_usage'"
        )
    )
    tracked = int(
        node.query(
            "SELECT value FROM system.metrics WHERE metric = 'MemoryTracking'"
        )
    )
    min_free_per_thread = (hard_limit - tracked) // 5
    assert min_free_per_thread > 0

    ground_truth, _ = query_with_profile_events(False, min_free_per_thread)

    try:
        node.query("SYSTEM DROP QUERY PLAN CACHE")
        allocate_memory_for_single_effective_thread(min_free_per_thread)
        low_seed, low_seed_events = query_with_profile_events(
            True, min_free_per_thread
        )
        node.query("SYSTEM FREE MEMORY")
        high_hit, high_hit_events = query_with_profile_events(
            True, min_free_per_thread
        )

        assert low_seed == ground_truth
        assert high_hit == ground_truth
        assert low_seed_events == (0, 1, 0, 0)
        assert high_hit_events == (1, 0, 1, 0)

        node.query("SYSTEM DROP QUERY PLAN CACHE")
        high_seed, high_seed_events = query_with_profile_events(
            True, min_free_per_thread
        )
        allocate_memory_for_single_effective_thread(min_free_per_thread)
        low_hit, low_hit_events = query_with_profile_events(
            True, min_free_per_thread
        )

        assert high_seed == ground_truth
        assert low_hit == ground_truth
        assert high_seed_events == (0, 1, 0, 0)
        assert low_hit_events == (1, 0, 1, 0)
    finally:
        node.query("SYSTEM FREE MEMORY")
        node.query("DROP TABLE IF EXISTS query_plan_cache_dynamic_threads")
