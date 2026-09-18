-- Coverage test for `topKThroughJoin.cpp` n-validation guards.
--
-- `tryTopKThroughJoin` (src/Processors/QueryPlan/Optimizations/topKThroughJoin.cpp)
-- contains two early-return guards on `n` (the LIMIT for sorting) that were
-- never triggered by CI:
--
--   lines 331-332  if (n == 0) return 0;
--     LIMIT 0 means there is nothing to preserve on the push-down side;
--     the optimisation is correctly skipped.
--
--   lines 336-337  if (settings.max_limit_for_top_k_optimization && n > max) return 0;
--     `query_plan_max_limit_for_top_k_optimization` caps how large an `n`
--     qualifies.  When the LIMIT exceeds the cap the push-down is suppressed.
--     The setting defaults to 0 (unlimited), so the branch is never reached
--     in normal CI runs.
--
-- Both guards are verified by comparing the number of `Sorting` steps in the
-- query plan.  When `topKThroughJoin` fires it inserts an extra Sort+Limit
-- below the join (two Sorting steps total); when blocked, only the outer
-- Sorting step remains (one step total).
--
-- The base case reuses the RIGHT JOIN pattern from
-- `04210_top_k_through_join_right_join_deferral.sql`: with RIGHT JOIN the
-- optimisation fires unconditionally (no second-pass deferral) whenever the
-- n-guards allow it, making the plan-level observation unambiguous.

SET enable_analyzer = 1;
SET query_plan_top_k_through_join = 1;

DROP TABLE IF EXISTS t_l_guards;
DROP TABLE IF EXISTS t_r_guards;

CREATE TABLE t_l_guards (k Int64, payload String) ENGINE = MergeTree() ORDER BY k;
CREATE TABLE t_r_guards (k Int64, value String)   ENGINE = MergeTree() ORDER BY k;

INSERT INTO t_l_guards SELECT number, repeat('x', 8) FROM numbers(200);
INSERT INTO t_r_guards SELECT number, repeat('y', 8) FROM numbers(200);

-- Base: LIMIT 10, unlimited cap → optimisation fires → 2 Sorting steps.
SELECT 'base_fires' AS label, countIf(explain LIKE '%Sorting%') AS sort_count
FROM (EXPLAIN actions = 0
    SELECT l.k, r.value
    FROM t_l_guards AS l RIGHT JOIN t_r_guards AS r ON r.k = l.k
    ORDER BY r.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false,
             query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0);

-- Guard 1 (lines 331-332): LIMIT 0  →  n == 0  →  optimisation skipped  → 1 Sorting step.
SELECT 'limit_zero_blocked' AS label, countIf(explain LIKE '%Sorting%') AS sort_count
FROM (EXPLAIN actions = 0
    SELECT l.k, r.value
    FROM t_l_guards AS l RIGHT JOIN t_r_guards AS r ON r.k = l.k
    ORDER BY r.k DESC LIMIT 0
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false,
             query_plan_max_limit_for_top_k_optimization = 0,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0);

-- LIMIT 0 correctness: query returns empty result.
SELECT 'limit_zero_result' AS label, count(*) AS rows
FROM (SELECT l.k, r.value
      FROM t_l_guards AS l RIGHT JOIN t_r_guards AS r ON r.k = l.k
      ORDER BY r.k DESC LIMIT 0
      SETTINGS enable_parallel_replicas = 0);

-- Guard 2 (lines 336-337): cap = 3, LIMIT 10  →  n (10) > cap (3)  →  skipped  → 1 Sorting step.
SELECT 'cap_exceeded_blocked' AS label, countIf(explain LIKE '%Sorting%') AS sort_count
FROM (EXPLAIN actions = 0
    SELECT l.k, r.value
    FROM t_l_guards AS l RIGHT JOIN t_r_guards AS r ON r.k = l.k
    ORDER BY r.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false,
             query_plan_max_limit_for_top_k_optimization = 3,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0);

-- Guard 2 — cap just met: cap = 10, LIMIT 10  →  n == cap (not strictly greater)  → fires  → 2 steps.
SELECT 'cap_exact_fires' AS label, countIf(explain LIKE '%Sorting%') AS sort_count
FROM (EXPLAIN actions = 0
    SELECT l.k, r.value
    FROM t_l_guards AS l RIGHT JOIN t_r_guards AS r ON r.k = l.k
    ORDER BY r.k DESC LIMIT 10
    SETTINGS optimize_read_in_order = 1,
             query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1,
             query_plan_join_swap_table = false,
             query_plan_max_limit_for_top_k_optimization = 10,
             enable_join_runtime_filters = 0, enable_lazy_columns_replication = 0,
             query_plan_optimize_lazy_materialization = 0, enable_parallel_replicas = 0);

-- Correctness: capped run still produces the right rows (result identical to uncapped).
SELECT 'cap_exceeded_result' AS label, count(*), max(rk), min(rk)
FROM (SELECT r.k AS rk, r.value
      FROM t_l_guards AS l RIGHT JOIN t_r_guards AS r ON r.k = l.k
      ORDER BY r.k DESC LIMIT 10
      SETTINGS query_plan_max_limit_for_top_k_optimization = 3,
               enable_parallel_replicas = 0);

DROP TABLE t_l_guards;
DROP TABLE t_r_guards;
