from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/primary_index_cache.xml"])

# Read the parts as they are, so that only the calculation of ranges needs memory.
SETTINGS = "split_intersecting_parts_ranges_into_layers_final = 0, use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0, max_threads = 4"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        # 12 parts, the primary index of each takes 9 MB, and the cache keeps none of them.
        node.query(
            """
            CREATE TABLE t (k String, v UInt64, s UInt64, INDEX i s TYPE minmax GRANULARITY 1)
            ENGINE = ReplacingMergeTree ORDER BY k
            SETTINGS index_granularity = 1, use_primary_key_cache = 1, primary_key_lazy_load = 1;
            SYSTEM STOP MERGES t;
            INSERT INTO t SELECT leftPad(toString(cityHash64(number) % 1000000000), 80, '0'), number, number FROM numbers(1200000)
            SETTINGS max_block_size = 100000, min_insert_block_size_rows = 100000, min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1;
            """
        )
        # 20 parts with the same keys in several of them.
        node.query(
            """
            CREATE TABLE d (k String, v UInt64, s UInt64, INDEX i s TYPE minmax GRANULARITY 1)
            ENGINE = ReplacingMergeTree(v) ORDER BY k
            SETTINGS index_granularity = 1, use_primary_key_cache = 1, primary_key_lazy_load = 1;
            SYSTEM STOP MERGES d;
            INSERT INTO d SELECT leftPad(toString(cityHash64(number % 15000)), 80, '0'), number, number % 15000 FROM numbers(20000)
            SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000, min_insert_block_size_bytes = 0, max_insert_threads = 1, max_threads = 1, optimize_on_insert = 0;
            """
        )
        yield cluster
    finally:
        cluster.shutdown()


def explain(where, max_memory_usage):
    return f"EXPLAIN PIPELINE SELECT sum(v) FROM t FINAL {where} SETTINGS max_memory_usage = '{max_memory_usage}', {SETTINGS}"


def test_values_of_indexes_are_accounted():
    assert node.query("SELECT count() FROM system.parts WHERE table = 't' AND active") == "12\n"

    error = node.query_and_get_error(explain("", "50M"))
    assert "MEMORY_LIMIT_EXCEEDED" in error, error

    assert "MergeTreeSelect" in node.query(explain("", "500M"))


def test_only_needed_values_of_indexes_are_kept():
    assert "MergeTreeSelect" in node.query(explain("WHERE s = 5", "50M"))
    assert node.query(f"SELECT sum(v) FROM t FINAL WHERE s = 5 SETTINGS {SETTINGS}") == "5\n"


def test_several_tables():
    # The first table does not use the cache.
    node.query(
        """
        CREATE TABLE IF NOT EXISTS n (k String, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS use_primary_key_cache = 0;
        TRUNCATE TABLE n;
        INSERT INTO n SELECT leftPad(toString(number * 100000), 80, '0'), number FROM numbers(10000);
        """
    )

    def query(max_memory_usage):
        return f"""EXPLAIN PIPELINE SELECT count() FROM n JOIN t ON n.k = t.k
            SETTINGS query_plan_join_shard_by_pk_ranges = 1, join_algorithm = 'full_sorting_merge', query_plan_join_swap_table = 0,
                max_threads = 4, max_memory_usage = '{max_memory_usage}'"""

    error = node.query_and_get_error(query("50M"))
    assert "MEMORY_LIMIT_EXCEEDED" in error, error

    assert "Join" in node.query(query("500M"))


def test_index_in_result_is_accounted():
    # The index of one part takes 9 MB, and the state of the aggregate function more than 15 MB. Memory that is freed, but was not accounted,
    # must not be subtracted from it.
    query = "SELECT uniqExact(k) FROM mergeTreeIndex(currentDatabase(), t) SETTINGS max_threads = 1, max_memory_usage = '{}'"

    error = node.query_and_get_error(query.format("15M"))
    assert "MEMORY_LIMIT_EXCEEDED" in error, error

    assert node.query(query.format("200M")) == node.query("SELECT uniqExact(k) FROM t")


def test_index_is_not_accounted_twice_for_server():
    # The primary index of the first part takes 200 MB.
    node.query(
        """
        SET allow_suspicious_fixed_string_types = 1, max_block_size = 2000, min_insert_block_size_rows = 2000, min_insert_block_size_bytes = 0;
        DROP TABLE IF EXISTS f SYNC;
        CREATE TABLE f (k FixedString(100000)) ENGINE = ReplacingMergeTree ORDER BY k
        SETTINGS index_granularity = 1, use_primary_key_cache = 1, primary_key_lazy_load = 1;
        SYSTEM STOP MERGES f;
        INSERT INTO f SELECT toString(number) FROM numbers(2000);
        INSERT INTO f VALUES ('5');
        """
    )
    query = f"EXPLAIN PIPELINE SELECT k FROM f FINAL SETTINGS {SETTINGS}, max_memory_usage = "
    error = node.query_and_get_error(query + "'150M'")
    assert "Query memory limit exceeded" in error, error

    def set_limit(limit):
        config = f"<clickhouse><max_server_memory_usage>{limit}</max_server_memory_usage></clickhouse>"
        node.replace_config("/etc/clickhouse-server/config.d/limit.xml", config)
        node.query("SYSTEM RELOAD CONFIG")

    # The index fits into the limit once, but not twice.
    used = node.query("SYSTEM RELOAD ASYNCHRONOUS METRICS; SELECT toInt64(value) FROM system.asynchronous_metrics WHERE metric = 'TrackedMemory'")
    try:
        set_limit(int(used) + 350000000)
        assert "MergeTreeSelect" in node.query(query + "0")
    finally:
        set_limit(0)


def test_materialized_view():
    node.query(
        f"""
        CREATE TABLE IF NOT EXISTS src (s String) ENGINE = Null;
        CREATE TABLE IF NOT EXISTS buf AS src ENGINE = Buffer(currentDatabase(), src, 1, 1000, 1000, 1, 1, 1, 1);
        CREATE MATERIALIZED VIEW IF NOT EXISTS mv ENGINE = Null AS
        SELECT length(s) AS x, v FROM src CROSS JOIN (SELECT v FROM t FINAL LIMIT 1) AS r SETTINGS {SETTINGS};
        """
    )
    query = "INSERT INTO {} SELECT number FROM numbers(10) SETTINGS max_memory_usage = '50M'"
    error = node.query_and_get_error(query.format("src"))
    assert "MEMORY_LIMIT_EXCEEDED" in error, error

    # What a buffer writes to its destination is not accounted.
    node.query(query.format("buf"))

    # The view frees blocks of the query while it is charged for the indexes, and nothing must be left on the query.
    drift = "SELECT sum(value) FROM system.events WHERE event = 'QueryMemoryDriftSettledBytes'"
    before = int(node.query(drift))
    node.query(
        """INSERT INTO src SELECT repeat('a', 1000) FROM numbers_mt(400000)
        SETTINGS max_block_size = 10000, min_insert_block_size_rows = 10000, min_insert_block_size_bytes = 0,
            max_threads = 4, max_insert_threads = 4, parallel_view_processing = 1, deduplicate_blocks_in_dependent_materialized_views = 0"""
    )
    assert int(node.query(drift)) - before < 10000000


CONDITIONS = [
    "s = 5",
    "s IN (5, 4999, 5000, 14999, 15000)",
    "s BETWEEN 4900 AND 5100",
    "s < 100 AND k > repeat('0', 61) || '5'",
    "s < 14000",
    "1",
]


def get_expected(where):
    return node.query(
        f"SELECT count(), sum(v) FROM (SELECT k, max(v) AS v, any(s) AS s FROM d GROUP BY k) WHERE {where}"
    )


@pytest.mark.parametrize("split", [0, 1])
@pytest.mark.parametrize("layers", [0, 1])
@pytest.mark.parametrize("where", CONDITIONS)
def test_results(split, layers, where):
    settings = f"""split_parts_ranges_into_intersecting_and_non_intersecting_final = {split},
        split_intersecting_parts_ranges_into_layers_final = {layers},
        use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0, max_threads = 4"""
    assert node.query(f"SELECT count(), sum(v) FROM d FINAL WHERE {where} SETTINGS {settings}") == get_expected(where)


def test_concurrent_queries():
    # The cache keeps about a half of the indexes, so the queries evict and reload the indexes that the others use.
    expected = {where: get_expected(where) for where in CONDITIONS}
    settings = "use_query_condition_cache = 0, use_skip_indexes_on_data_read = 0, max_threads = 4"
    conditions = CONDITIONS * 4

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = pool.map(
            lambda where: node.query(f"SELECT count(), sum(v) FROM d FINAL WHERE {where} SETTINGS {settings}"), conditions
        )
        assert list(results) == [expected[where] for where in conditions]

    # The indexes are much larger than the cache here, so they are checked several times while they are loaded.
    with ThreadPoolExecutor(max_workers=4) as pool:
        results = pool.map(
            lambda s: node.query(f"SELECT sum(v) FROM t FINAL WHERE s = {s} SETTINGS max_memory_usage = '50M', {SETTINGS}"), range(8)
        )
        plans = pool.map(lambda _: node.query(explain("", "500M")), range(8))
        assert list(results) == [f"{s}\n" for s in range(8)]
        assert all("MergeTreeSelect" in plan for plan in plans)
