-- Tags: no-parallel-replicas
-- no-parallel-replicas: the no-ORDER-BY promotion needs `LimitStep` directly
-- above `AggregatingStep` in a single-stage plan, so under parallel replicas
-- the optimization never engages and the sort-processor assertions would fail.

-- The top-K optimization does not apply to serialized plans; pin the setting
-- so the assertions hold in the distributed-plan suite.
SET serialize_query_plan = 0;

-- When the hash-table size statistics from a previous run say the group count
-- cannot reach the top-K heap's capacity, the plan-level gate abandons the
-- heap.  For a query without its own ORDER BY the optimization synthesized a
-- Sorting step solely for the heap; the gate must remove that sort together
-- with the heap, otherwise the "not profitable" fallback would pay for a sort
-- the original query never asked for.

-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;
SET collect_hash_table_stats_during_aggregation = 1;
-- The trivial GROUP BY ... LIMIT rewrite sets max_rows_to_group_by, which
-- disables the top-K optimization for aggregate-free projections; keep it off.
SET optimize_trivial_group_by_limit_query = 0;
-- The size hint counts a key once per thread that saw it; a single thread
-- keeps the recorded sum at the true group count.
SET max_threads = 1;
SET log_processors_profiles = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS t_top_k_size_hint;

CREATE TABLE t_top_k_size_hint (k UInt64, val UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_top_k_size_hint SELECT number % 3, number FROM numbers(100000);

-- First run: no size hint exists yet, so the heap engages and the synthesized
-- sort executes.  With 3 groups against a capacity of 5 the heap never rejects
-- anything, so the true sizes are recorded for the next run.
SELECT k, count() FROM t_top_k_size_hint GROUP BY k LIMIT 5
SETTINGS log_comment = '04653_top_k_size_hint_first' FORMAT Null;

-- Second run: the size hint says 3 groups cannot reach 5 * 1.5, so the gate
-- abandons the heap and removes the synthesized sort, restoring the
-- unoptimized plan.
SELECT k, count() FROM t_top_k_size_hint GROUP BY k LIMIT 5
SETTINGS log_comment = '04653_top_k_size_hint_second' FORMAT Null;

-- The gated plan still returns every group.
SELECT 'gated_run_rows';
SELECT count() FROM (SELECT k, count() FROM t_top_k_size_hint GROUP BY k LIMIT 5);

SYSTEM FLUSH LOGS query_log, processors_profile_log;

SELECT 'first_run_has_sort_processors';
SELECT countDistinct(name) > 0 FROM system.processors_profile_log
WHERE event_date >= yesterday() AND name LIKE '%Sorting%' AND query_id IN
(
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
        AND log_comment = '04653_top_k_size_hint_first' AND type = 'QueryFinish'
);

SELECT 'second_run_has_sort_processors';
SELECT countDistinct(name) > 0 FROM system.processors_profile_log
WHERE event_date >= yesterday() AND name LIKE '%Sorting%' AND query_id IN
(
    SELECT query_id FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
        AND log_comment = '04653_top_k_size_hint_second' AND type = 'QueryFinish'
);

DROP TABLE t_top_k_size_hint;
