-- The top-K heap must reach the *partial* aggregation of a distributed query
-- (Pattern 1 only: `GROUP BY <keys> ORDER BY <prefix of keys> LIMIT N`).  Each
-- shard plans the query text locally at stage `WithMergeableState`; the plan
-- has no LimitStep/SortingStep there, so the parameters are derived from the
-- analyzed query in `Planner.cpp` (`applyTopKPushdownToPartialAggregation`).
SET prefer_localhost_replica = 0;  -- both shards go over TCP and appear as secondary queries in query_log
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;

-- Correctness: identical results with the optimization on and off.  Two
-- "shards" read the same data, so every count doubles.
SELECT k, count() FROM remote('127.0.0.1,localhost', view(
    SELECT intDiv(number, 10) AS k FROM numbers(100000)
)) GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1, log_comment = '04496_partial_agg_on';

SELECT k, count() FROM remote('127.0.0.1,localhost', view(
    SELECT intDiv(number, 10) AS k FROM numbers(100000)
)) GROUP BY k ORDER BY k ASC LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0, log_comment = '04496_partial_agg_off';

-- Composite key, full ORDER BY match with mixed directions.
SELECT k1, k2, count() FROM remote('127.0.0.1,localhost', view(
    SELECT intDiv(number, 100) AS k1, number % 7 AS k2 FROM numbers(100000)
)) GROUP BY k1, k2 ORDER BY k1 DESC, k2 ASC LIMIT 3
SETTINGS enable_group_by_top_k_optimization = 1;

SYSTEM FLUSH LOGS query_log;

-- The shard-side (secondary, non-initial) queries must have skipped rows via
-- the heap when the optimization is on, and none when it is off.
SELECT
    countIf(NOT is_initial_query) AS shard_queries,
    sum(ProfileEvents['AggregationTopKRowsSkipped']) > 0 AS shards_skipped
FROM system.query_log
WHERE type = 'QueryFinish'
    AND event_date >= yesterday()
    AND initial_query_id IN
    (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND log_comment = '04496_partial_agg_on'
            AND is_initial_query
            AND type = 'QueryFinish'
            AND event_date >= yesterday()
    );

SELECT sum(ProfileEvents['AggregationTopKRowsSkipped']) AS off_skipped
FROM system.query_log
WHERE type = 'QueryFinish'
    AND event_date >= yesterday()
    AND initial_query_id IN
    (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase()
            AND log_comment = '04496_partial_agg_off'
            AND is_initial_query
            AND type = 'QueryFinish'
            AND event_date >= yesterday()
    );
