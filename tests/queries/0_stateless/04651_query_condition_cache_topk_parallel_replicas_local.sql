-- Tags: long, no-parallel, no-parallel-replicas
-- Tag no-parallel: Messes with internal cache
-- Tag no-parallel-replicas: The test sets up parallel replicas explicitly
-- Tag long: needs ~1M rows for the QCC to populate.
--
-- Companion of `04628_query_condition_cache_topk_default_off` and
-- `04647_query_condition_cache_topk_cloned_subplan`, covering the local parallel replicas plan.
--
-- `createLocalParallelReplicasReadingStep` replaces a `ReadFromMergeTree` step in place with a
-- freshly constructed one. Like `ReadFromMergeTree::clone`, it must carry over `top_k_filter_info`
-- and `allow_query_condition_cache`: a read that was stamped by `tryOptimizeTopK` would otherwise
-- look like a plain read after the replacement and, with `use_query_condition_cache_for_top_k = 0`,
-- would consult and populate the query condition cache under the unsalted condition hash.
--
-- Today the local plan is built by a fresh interpreter run and the shipped fragment puts the split
-- step directly above the read, so the replaced steps are not stamped yet and the invariant holds
-- trivially. The test pins the observable contract of the shape - results and cache use - so that a
-- future change which starts replacing stamped reads cannot silently reintroduce unsalted reuse.

SET allow_experimental_analyzer = 1;
SET use_query_condition_cache = 1;
-- Pin the gate to its default value: this test covers the default-off contract.
SET use_query_condition_cache_for_top_k = 0;
SET use_top_k_dynamic_filtering = 1;
SET use_skip_indexes_for_top_k = 1;
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET optimize_move_to_prewhere = 0;
SET max_threads = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_min_number_of_rows_per_replica = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt32, v1 UInt32, v2 UInt32) ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64,
         min_bytes_for_wide_part = 0,
         min_bytes_for_full_part_storage = 0,
         add_minmax_index_for_numeric_columns = 0;

-- Query condition cache entries are keyed by part, so a background merge of `tab` would invalidate
-- entries primed by an earlier query and make the reuse assertions below nondeterministic.
SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, number, number FROM numbers(1_000_000) SETTINGS enable_parallel_replicas = 0;

SELECT '--- The local parallel replicas plan returns the same rows as the plain read';

SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) ORDER BY v1 ASC LIMIT 5 SETTINGS enable_parallel_replicas = 0;
SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) ORDER BY v1 ASC LIMIT 5;

SELECT '--- A TopK read of the local parallel replicas plan writes no QCC entry';
SYSTEM CLEAR QUERY CONDITION CACHE;

SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) ORDER BY v1 ASC LIMIT 5 FORMAT Null
    SETTINGS log_comment = '04651_topk_pr_write';

-- Expected: 0 entries - every write side is gated off for a TopK read while the gate is off.
SELECT count() FROM system.query_condition_cache;

SELECT '--- A TopK read of the local parallel replicas plan must not reuse a plain entry';
SYSTEM CLEAR QUERY CONDITION CACHE;

-- Prime an entry with a plain (non-TopK) read of the same predicate.
SELECT count() FROM tab WHERE v2 IN (10000, 11000, 12000) FORMAT Null SETTINGS log_comment = '04651_plain_prime';
-- The plain read reuses its own entry: positive control that the entry exists.
SELECT count() FROM tab WHERE v2 IN (10000, 11000, 12000) FORMAT Null SETTINGS log_comment = '04651_plain_reuse';
-- The TopK read going through the local parallel replicas plan must not consult that entry.
SELECT v1 FROM tab WHERE v2 IN (10000, 11000, 12000) ORDER BY v1 ASC LIMIT 5 FORMAT Null
    SETTINGS log_comment = '04651_topk_pr_after_prime';

SYSTEM FLUSH LOGS query_log;

-- Column: (any QCC hit). Expected: TopK reads = 0, plain-prime = 0, plain-reuse = 1.
SELECT
    log_comment,
    ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment IN ('04651_topk_pr_write', '04651_plain_prime', '04651_plain_reuse', '04651_topk_pr_after_prime')
ORDER BY event_time_microseconds;

DROP TABLE tab;
