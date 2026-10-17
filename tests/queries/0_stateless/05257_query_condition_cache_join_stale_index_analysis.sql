-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests that a JOIN, also one read by parallel replicas, does not stop the query condition cache from
-- pruning granules for the joined table's own WHERE condition. The condition is on a column outside the
-- primary key, so the primary index cannot prune it and the cache is the only thing that can; a repeated
-- query must therefore read fewer granules the second time, with or without a JOIN above the read.

SET enable_parallel_replicas = 0;
SET parallel_replicas_local_plan = 1;
SET use_query_condition_cache = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
-- A randomized join order substitutes made-up row counts, which can pick a different plan and move the
-- condition out of the read's PREWHERE, so the plan anchors below would stop describing the query.
SET query_plan_optimize_join_order_randomize = 0;
-- A join runtime filter prunes on the join key, which is the primary key here, so it reaches the one
-- granule the condition matches on the FIRST run and leaves the cache nothing to record. It is an
-- independent pruner, tested elsewhere; switching it off is what makes the counts below about the cache.
SET enable_join_runtime_filters = 0;
-- Join reordering is what makes the first arm interesting, so it is pinned to its default rather than
-- left to be switched off, which is what the second arm does deliberately.
SET query_plan_optimize_join_order_limit = 10;
-- The test server's profile sets a row limit. With a limit that throws, join reordering keeps no analysis
-- of the read, which is analyzed again after the condition moves to PREWHERE and so already pruned before
-- this fix. The default, no limit, is the case this test is about.
SET max_rows_to_read = 0;

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS dim;
DROP TABLE IF EXISTS ptab;

CREATE TABLE tab (k Int32, v Int64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE dim (k Int32) ENGINE = MergeTree ORDER BY k;

INSERT INTO tab SELECT number, number FROM numbers(500000);
INSERT INTO dim SELECT 7;

-- The two plan anchors pin the shape the granule counts below depend on: the condition sits in the
-- read's PREWHERE, and nothing filters above the read. They run before the cache is cleared because an
-- EXPLAIN may write entries of its own. `pretty = 0` is required because the pretty renderer replaces a
-- filter's column name with an annotation, and that name is the anchor.
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT count() FROM tab INNER JOIN dim ON tab.k = dim.k WHERE tab.v = 7
    SETTINGS query_plan_max_step_description_length = 1000)
WHERE explain ILIKE '%prewhere filter column: %v, 7\_%';
SELECT count() FROM (EXPLAIN actions = 0, pretty = 0 SELECT count() FROM tab INNER JOIN dim ON tab.k = dim.k WHERE tab.v = 7
    SETTINGS query_plan_max_step_description_length = 1000)
WHERE trimLeft(explain) ILIKE 'Filter%';

SELECT '-- a JOIN must not stop the cache from pruning the second run';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM tab INNER JOIN dim ON tab.k = dim.k WHERE tab.v = 7 SETTINGS log_comment = 'qcc_join_r1';
SELECT count() FROM tab INNER JOIN dim ON tab.k = dim.k WHERE tab.v = 7 SETTINGS log_comment = 'qcc_join_r2';
SYSTEM FLUSH LOGS query_log;
-- Run 1 populates the cache and reads every mark, run 2 hits it and reads at most three: the matching
-- granule, the one of `dim`, and on a compact part sometimes the next granule. Every value is printed per
-- run, so a reference diff shows which property went missing. The condition is looked up once as the
-- WHERE and once as the PREWHERE it was moved to, each lookup counting one hit or miss, so run 1 misses
-- twice and run 2 hits once and misses once. Run 2's last value counts index analyses, one per table:
-- this read must not pay a second index analysis.
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       ProfileEvents['SelectedMarks'] = ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_join_r1'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       ProfileEvents['SelectedMarks'] <= 3,
       ProfileEvents['IndexAnalysisRounds']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_join_r2'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- a JOIN read by parallel replicas must prune the second run too';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM tab INNER JOIN dim ON tab.k = dim.k WHERE tab.v = 7
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_plan_based = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 0,
    log_comment = 'qcc_pr_join_r1';
SELECT count() FROM tab INNER JOIN dim ON tab.k = dim.k WHERE tab.v = 7
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_local_plan = 1, parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_index_analysis_only_on_coordinator = 1,
    parallel_replicas_plan_based = 0, automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 0,
    log_comment = 'qcc_pr_join_r2';
SYSTEM FLUSH LOGS query_log;
-- Run 2's last value shows that the read really went through parallel replicas.
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       ProfileEvents['SelectedMarks'] = ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_pr_join_r1'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       ProfileEvents['SelectedMarks'] <= 3,
       ProfileEvents['IndexAnalysisRounds'],
       ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_pr_join_r2'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- a TopK read under a JOIN must prune the second run too';
-- `ORDER BY ... LIMIT` makes the joined read a TopK read, whose PREWHERE gets `__topKFilter` and keys the cache on it.
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT count() FROM (SELECT k FROM tab WHERE v = 7 ORDER BY v LIMIT 3) AS t INNER JOIN dim ON t.k = dim.k
    SETTINGS use_top_k_dynamic_filtering = 1, use_query_condition_cache_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 0, query_plan_max_step_description_length = 1000)
WHERE explain ILIKE '%prewhere filter column: and(\_\_topKFilter(v), %';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM (SELECT k FROM tab WHERE v = 7 ORDER BY v LIMIT 3) AS t INNER JOIN dim ON t.k = dim.k
SETTINGS use_top_k_dynamic_filtering = 1, use_query_condition_cache_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 0, log_comment = 'qcc_topk_join_r1';
SELECT count() FROM (SELECT k FROM tab WHERE v = 7 ORDER BY v LIMIT 3) AS t INNER JOIN dim ON t.k = dim.k
SETTINGS use_top_k_dynamic_filtering = 1, use_query_condition_cache_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 0, log_comment = 'qcc_topk_join_r2';
SYSTEM FLUSH LOGS query_log;
-- The analysis kept for join reordering looks up the WHERE and the `__topKFilter` still waiting for its PREWHERE,
-- and the PREWHERE the condition moves to is looked up once more, so run 1 misses three times.
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       ProfileEvents['SelectedMarks'] = ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_topk_join_r1'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       ProfileEvents['SelectedMarks'] <= 3,
       ProfileEvents['IndexAnalysisRounds']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_topk_join_r2'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- control: the same JOIN without join reordering already pruned';
-- Join reordering is what asks the read for a row estimate, so switching it off is the one arm that
-- pruned before this fix. Without it a green arm above could not be told from a fixture that never
-- populates the cache at all.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM tab INNER JOIN dim ON tab.k = dim.k WHERE tab.v = 7
SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = 'qcc_nojoinorder_r1';
SELECT count() FROM tab INNER JOIN dim ON tab.k = dim.k WHERE tab.v = 7
SETTINGS query_plan_optimize_join_order_limit = 0, log_comment = 'qcc_nojoinorder_r2';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['SelectedMarks'] = ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_nojoinorder_r1'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['SelectedMarks'] <= 3
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_nojoinorder_r2'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- control: the same condition without a JOIN still prunes';
-- Reading `k` too keeps the condition in PREWHERE, as in the JOIN arms above.
SYSTEM DROP QUERY CONDITION CACHE;
SELECT sum(k) FROM tab WHERE v = 7 SETTINGS log_comment = 'qcc_nojoin_r1';
SELECT sum(k) FROM tab WHERE v = 7 SETTINGS log_comment = 'qcc_nojoin_r2';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['SelectedMarks'] = ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_nojoin_r1'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['SelectedMarks'] <= 2
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_nojoin_r2'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- a GROUP BY TopK read analyzed for a projection it does not use must prune the second run too';
-- The projection matches the query but cannot use the condition on `k`, so it is analyzed and rejected, and its analysis
-- of the table is kept. Only then the `GROUP BY ... ORDER BY ... LIMIT` heap adds `__topKFilter(w)` to the PREWHERE.
CREATE TABLE ptab (k Int32, v Int64, w Int64, PROJECTION p (SELECT w, v, k, count() GROUP BY w, v, k)) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;
INSERT INTO ptab SELECT number, number % 10, (number * 7919) % 1000003 FROM numbers(500000);
SET optimize_use_projections = 1, enable_group_by_top_k_optimization = 1, enable_group_by_top_k_dynamic_filtering = 1,
    use_top_k_dynamic_filtering = 1, use_query_condition_cache_for_top_k = 1, query_plan_max_limit_for_top_k_optimization = 0,
    max_rows_to_group_by = 0, optimize_aggregation_in_order = 0, optimize_read_in_order = 0, serialize_query_plan = 0,
    max_threads = 1, max_block_size = 8192;
SELECT count() > 0 FROM (EXPLAIN actions = 1, pretty = 0 SELECT w, count() FROM ptab WHERE k < 100000 AND v > 5 GROUP BY w ORDER BY w LIMIT 3
    SETTINGS query_plan_max_step_description_length = 1000)
WHERE explain ILIKE '%prewhere filter column: and(\_\_topKFilter(w), %';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT w, count() FROM ptab WHERE k < 100000 AND v > 5 GROUP BY w ORDER BY w LIMIT 3 SETTINGS log_comment = 'qcc_gbtopk_proj_r1' FORMAT Null;
SELECT w, count() FROM ptab WHERE k < 100000 AND v > 5 GROUP BY w ORDER BY w LIMIT 3 SETTINGS log_comment = 'qcc_gbtopk_proj_r2';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT w, count() FROM ptab WHERE k < 100000 AND v > 5 GROUP BY w ORDER BY w LIMIT 3
SETTINGS optimize_use_projections = 0, log_comment = 'qcc_gbtopk_noproj_r1' FORMAT Null;
SELECT w, count() FROM ptab WHERE k < 100000 AND v > 5 GROUP BY w ORDER BY w LIMIT 3
SETTINGS optimize_use_projections = 0, log_comment = 'qcc_gbtopk_noproj_r2';
SYSTEM FLUSH LOGS query_log;
-- Per arm, run 2's hits, misses, whether it read fewer marks than run 1, index analyses (the projection's adds one), and
-- whether it read the table, not the projection. The arm without the projection is the control that already pruned.
SELECT replaceOne(r2.log_comment, '_r2', ''), r2.ProfileEvents['QueryConditionCacheHits'], r2.ProfileEvents['QueryConditionCacheMisses'],
       r2.ProfileEvents['SelectedMarks'] < r1.ProfileEvents['SelectedMarks'], r2.ProfileEvents['IndexAnalysisRounds'], empty(r2.projections)
FROM system.query_log AS r2 INNER JOIN system.query_log AS r1 ON replaceOne(r2.log_comment, '_r2', '_r1') = r1.log_comment
WHERE r2.event_date >= yesterday() AND r1.event_date >= yesterday() AND r2.type = 'QueryFinish' AND r1.type = 'QueryFinish'
  AND r2.current_database = currentDatabase() AND r1.current_database = currentDatabase()
  AND r2.log_comment IN ('qcc_gbtopk_proj_r2', 'qcc_gbtopk_noproj_r2')
ORDER BY r2.log_comment DESC;
-- Both warm runs read the same marks.
SELECT uniqExact(ProfileEvents['SelectedMarks']) FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
  AND log_comment IN ('qcc_gbtopk_proj_r2', 'qcc_gbtopk_noproj_r2');

DROP TABLE ptab;
DROP TABLE dim;
DROP TABLE tab;
