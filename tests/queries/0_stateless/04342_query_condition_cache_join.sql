-- Tags: no-parallel-replicas
-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/104985
--
-- A self-join whose WHERE is moved to PREWHERE must still hit the query condition cache on a warm run.
-- Before the fix the join-branch index analysis was cached before optimizePrewhere ran, so the cache
-- was read under the WHERE-filter key while the per-granule entries were written under the PREWHERE key;
-- the two keys never matched and every warm run re-scanned the whole table.
--
-- A unique marker in the predicate keeps the cache keys disjoint from other tests, so no global
-- SYSTEM DROP QUERY CONDITION CACHE is needed and the test stays parallel-safe.

SET enable_analyzer = 1;
SET use_query_condition_cache = 1;
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS t_qcc_join;

CREATE TABLE t_qcc_join (x String, k UInt32)
ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;

-- 1 mio rows: the QCC does not cache anything for small tables.
INSERT INTO t_qcc_join SELECT concat('x-', toString(number % 1000)), number FROM numbers(1000000);

-- Warm-up run populates the cache. The predicate matches no rows, so the right side is empty.
SELECT count() FROM (
    SELECT a.k FROM t_qcc_join AS a
    INNER JOIN t_qcc_join AS b ON a.k = b.k
    WHERE a.x = 'no_such_value_104985' AND b.x = 'no_such_value_104985'
) FORMAT Null SETTINGS use_query_condition_cache = 1, log_comment = 'qcc_join_104985';

-- Warm run: must be served from the query condition cache.
SELECT count() FROM (
    SELECT a.k FROM t_qcc_join AS a
    INNER JOIN t_qcc_join AS b ON a.k = b.k
    WHERE a.x = 'no_such_value_104985' AND b.x = 'no_such_value_104985'
) FORMAT Null SETTINGS use_query_condition_cache = 1, log_comment = 'qcc_join_104985';

SYSTEM FLUSH LOGS query_log;

-- One row per execution, ordered in time. Cold run: cache miss, full scan. Warm run: cache hit,
-- the probe side reads 0 rows (all granules pruned). Expected:
--   0  1   (cold: no hit, rows were read)
--   1  1   (warm: hit, 0 rows read)
SELECT
    ProfileEvents['QueryConditionCacheHits'] > 0,
    ProfileEvents['SelectedRows'] < 1000000
FROM system.query_log
WHERE event_date >= yesterday()
    AND type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'qcc_join_104985'
ORDER BY event_time_microseconds;

DROP TABLE t_qcc_join;
