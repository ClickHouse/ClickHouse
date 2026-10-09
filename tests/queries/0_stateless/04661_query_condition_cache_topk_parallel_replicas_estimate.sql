-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag no-parallel-replicas: The test sets up parallel replicas explicitly
-- Tag long: needs ~1M rows for the QCC to populate.
--
-- Companion of `04628_query_condition_cache_topk_default_off` and
-- `04651_query_condition_cache_topk_parallel_replicas_local`, covering the pre-plan estimate that
-- sizes automatic parallel replicas (`parallel_replicas_min_number_of_rows_per_replica > 0`).
--
-- That estimate runs index analysis on the `ReadFromMergeTree` step while the planner builds the join
-- tree, i.e. before `tryOptimizeTopK` stamps the read, and its result is what the executed read uses.
-- Analyzed as an apparent plain read, it would consult plain `SELECT ... WHERE` entries and record its
-- index-analysis exclusions back under the plain condition hash - the very interaction that
-- `use_query_condition_cache_for_top_k = 0` is supposed to gate off. So for a query which may still
-- become a TopK read, the estimate must not touch the query condition cache at all.
--
-- The TopK stamp uses the skip-index-only shape (dynamic filtering off, minmax index on the sort
-- column `v1`), so any entry observed below comes from index analysis and not from a `__topKFilter`
-- prewhere. The minmax index on `v2` is what makes index analysis drop granules, and therefore have
-- exclusions to record.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
-- Pin the gate to its default value: this test covers the default-off contract.
SET use_query_condition_cache_for_top_k = 0;
SET use_top_k_dynamic_filtering = 0;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_move_to_prewhere = 0;
SET max_threads = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
-- Query-tree based parallel replicas: this is the mechanism that runs the pre-plan estimate.
SET parallel_replicas_plan_based = 0;
SET automatic_parallel_replicas_mode = 0;
-- Any positive value enables the estimate. A value above the table size makes the estimate conclude
-- that one replica is enough, so the queries below execute locally: what is asserted is then the cache
-- interaction of the estimate itself, not of reads on the follower replicas.
SET parallel_replicas_min_number_of_rows_per_replica = 100000000;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt32,
    v1 UInt32,
    v2 UInt32,
    INDEX idx_v1 v1 TYPE minmax GRANULARITY 1,
    INDEX idx_v2 v2 TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

-- Query condition cache entries are keyed by part, so a background merge of `tab` would invalidate
-- entries primed by an earlier query and make the assertions below nondeterministic.
SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, number, number FROM numbers(1_000_000) SETTINGS enable_parallel_replicas = 0;

SELECT '--- The estimated plan returns the same rows as the plain read';

SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 SETTINGS enable_parallel_replicas = 0;
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5;

SELECT '--- A TopK read sized by the pre-plan estimate writes no QCC entry';
SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null
    SETTINGS log_comment = '04661_topk_estimate_write';

-- Expected: 0 entries - neither the estimate nor the executed read may record exclusions.
SELECT count() FROM system.query_condition_cache;

SELECT '--- A TopK read sized by the pre-plan estimate must not reuse a plain entry';
SYSTEM CLEAR QUERY CONDITION CACHE;

-- Prime an entry with a plain (non-TopK) read of the same predicate. Its own hit count is not asserted:
-- a plain query under this estimate analyzes the read twice (once for the estimate, once after
-- `optimizePrimaryKeyConditionAndLimit` re-applies the filters), so it reuses what it just wrote.
SELECT count() FROM tab WHERE v2 = 10000 FORMAT Null SETTINGS log_comment = '04661_plain_prime';
-- The plain read reuses the entry: positive control that it exists and is matchable.
SELECT count() FROM tab WHERE v2 = 10000 FORMAT Null SETTINGS log_comment = '04661_plain_reuse';
-- The TopK read must not consult that entry, neither in the estimate nor in the executed read.
SELECT v1 FROM tab WHERE v2 = 10000 ORDER BY v1 ASC LIMIT 5 FORMAT Null
    SETTINGS log_comment = '04661_topk_estimate_after_prime';

SYSTEM FLUSH LOGS query_log;

-- Column: (any QCC hit). Expected: TopK reads = 0, plain-reuse = 1.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04661_topk_estimate_write', '04661_plain_reuse', '04661_topk_estimate_after_prime')
ORDER BY event_time_microseconds;

DROP TABLE tab;
