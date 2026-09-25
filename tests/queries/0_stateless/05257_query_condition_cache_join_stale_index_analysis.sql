-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

-- Tests that a JOIN does not stop the query condition cache from pruning granules for the joined
-- table's own WHERE condition. The condition is on a column outside the primary key, so the primary
-- index cannot prune it and the cache is the only thing that can; a repeated query must therefore read
-- fewer granules the second time, with or without a JOIN above the read.

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

DROP TABLE IF EXISTS tab;
DROP TABLE IF EXISTS dim;

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
-- Run 1 populates and prunes nothing, run 2 hits and prunes. Every value is printed per run, so a
-- reference diff shows which property went missing. The condition is looked up once as the WHERE and
-- once as the PREWHERE it was moved to, each lookup counting one hit or miss, so run 1 misses twice and
-- run 2 hits once and misses once. Run 2's last value counts index analyses, one per table: this read
-- must not pay a second index analysis.
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       toInt32(ProfileEvents['SelectedMarks']) * 4 < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_join_r1'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'],
       ProfileEvents['QueryConditionCacheMisses'],
       toInt32(ProfileEvents['SelectedMarks']) * 4 < toInt32(ProfileEvents['SelectedMarksTotal']),
       ProfileEvents['IndexAnalysisRounds']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_join_r2'
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
       toInt32(ProfileEvents['SelectedMarks']) * 4 < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_nojoinorder_r1'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'],
       toInt32(ProfileEvents['SelectedMarks']) * 4 < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_nojoinorder_r2'
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT '-- control: the same condition without a JOIN still prunes';
SYSTEM DROP QUERY CONDITION CACHE;
SELECT count() FROM tab WHERE v = 7 SETTINGS log_comment = 'qcc_nojoin_r1';
SELECT count() FROM tab WHERE v = 7 SETTINGS log_comment = 'qcc_nojoin_r2';
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['QueryConditionCacheHits'],
       toInt32(ProfileEvents['SelectedMarks']) * 4 < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_nojoin_r1'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT ProfileEvents['QueryConditionCacheHits'],
       toInt32(ProfileEvents['SelectedMarks']) * 4 < toInt32(ProfileEvents['SelectedMarksTotal'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600 AND type = 'QueryFinish'
  AND current_database = currentDatabase() AND log_comment = 'qcc_nojoin_r2'
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE dim;
DROP TABLE tab;
