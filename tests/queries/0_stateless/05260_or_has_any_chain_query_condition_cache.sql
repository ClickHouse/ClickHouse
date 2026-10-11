-- Tags: no-parallel
-- Tag no-parallel: asserts a hit in the instance-wide query condition cache, which a sibling test's SYSTEM DROP QUERY CONDITION CACHE can evict

-- `optimize_or_has_any_chain` folds the needles of `hasAny(x, [hostName()]) OR hasAny(x, [hostName() || 'x'])` into a
-- single constant. The merged constant must stay non-deterministic, otherwise the filter would be stored in the query
-- condition cache although its value depends on the server. The deterministic chain is a control that the cache is used.

-- w/o local plan for parallel replicas the filter steps are executed as part of remote queries
SET parallel_replicas_local_plan = 1;

SET enable_analyzer = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (id UInt64, a Array(String)) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 100, add_minmax_index_for_numeric_columns = 0;
INSERT INTO tab SELECT number, [toString(number)] FROM numbers(10000);

-- both chains are merged into a single hasAny
SELECT sum(countSubstrings(explain, 'hasAny(')) FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE hasAny(a, ['1']) OR hasAny(a, ['2']));
SELECT sum(countSubstrings(explain, 'hasAny(')) FROM (EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM tab WHERE hasAny(a, [hostName()]) OR hasAny(a, [hostName() || 'x']));

-- small blocks, so that granules without matches produce empty chunks, which are what the cache records
SELECT count() FROM tab WHERE hasAny(a, ['1']) OR hasAny(a, ['2']) SETTINGS use_query_condition_cache = 1, max_block_size = 100, log_comment = '05260_deterministic';
SELECT count() FROM tab WHERE hasAny(a, ['1']) OR hasAny(a, ['2']) SETTINGS use_query_condition_cache = 1, max_block_size = 100, log_comment = '05260_deterministic';
SELECT count() FROM tab WHERE hasAny(a, [hostName()]) OR hasAny(a, [hostName() || 'x']) SETTINGS use_query_condition_cache = 1, max_block_size = 100, log_comment = '05260_non_deterministic';
SELECT count() FROM tab WHERE hasAny(a, [hostName()]) OR hasAny(a, [hostName() || 'x']) SETTINGS use_query_condition_cache = 1, max_block_size = 100, log_comment = '05260_non_deterministic';

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05260_deterministic', '05260_non_deterministic')
ORDER BY event_time_microseconds;

DROP TABLE tab;
