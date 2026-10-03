-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with internal cache.
-- no-random-* / no-parallel-replicas: Flaky check must not randomize settings or inject parallel replicas; breaks GROUP BY correctness and cache ProfileEvents.

-- Partial aggregate cache: a `View` is read through its own inner query, which can hide a `JOIN` that the
-- outer query AST does not show. Changing the joined table must not reuse stale per-part states, so the
-- cache is not used for a query over a `View` at all.

SYSTEM DROP AGGREGATE CACHE;

DROP VIEW IF EXISTS test_partial_agg_cache_view_v;
DROP TABLE IF EXISTS test_partial_agg_cache_view_fact;
DROP TABLE IF EXISTS test_partial_agg_cache_view_dim;

CREATE TABLE test_partial_agg_cache_view_fact (k UInt32, v Int64) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE test_partial_agg_cache_view_dim (k UInt32, m Int64) ENGINE = MergeTree() ORDER BY k;

CREATE VIEW test_partial_agg_cache_view_v AS
SELECT f.k AS k, f.v * d.m AS v
FROM test_partial_agg_cache_view_fact AS f
INNER JOIN test_partial_agg_cache_view_dim AS d USING k;

SYSTEM STOP MERGES test_partial_agg_cache_view_fact;

SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';
SET use_partial_aggregate_cache = 1;

INSERT INTO test_partial_agg_cache_view_fact VALUES (1, 10), (2, 20);
INSERT INTO test_partial_agg_cache_view_dim VALUES (1, 1), (2, 1);

SELECT '--- Multiplier 1';
SELECT k, sum(v) FROM test_partial_agg_cache_view_v GROUP BY k ORDER BY k;
SELECT k, sum(v) FROM test_partial_agg_cache_view_v GROUP BY k ORDER BY k SETTINGS log_comment = 'test_partial_agg_cache_view_repeat';

TRUNCATE TABLE test_partial_agg_cache_view_dim;
INSERT INTO test_partial_agg_cache_view_dim VALUES (1, 3), (2, 3);

SELECT '--- Only the joined table changes, multiplier 3';
SELECT k, sum(v) FROM test_partial_agg_cache_view_v GROUP BY k ORDER BY k;

SELECT '--- No cache hits for the query over the view';
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['PartialAggregateCacheHits']
FROM system.query_log
WHERE
    type = 'QueryFinish'
    AND current_database = currentDatabase()
    AND log_comment = 'test_partial_agg_cache_view_repeat'
    AND is_initial_query = 1
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP VIEW test_partial_agg_cache_view_v;
DROP TABLE test_partial_agg_cache_view_fact;
DROP TABLE test_partial_agg_cache_view_dim;
