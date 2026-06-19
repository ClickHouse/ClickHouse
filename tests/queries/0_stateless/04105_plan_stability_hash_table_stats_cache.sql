-- The `HashTablesStatistics` cache records post-runtime-filter `source_rows` for
-- each executed hash join, keyed on a hash of the right subtree. When a later
-- execution of the same query reads that cache, the DP must not clamp
-- `estimated_rows` to the cached (biased-downward) value — otherwise the join
-- order or build-side choice can flip between cold and warm runs.
-- Regression guard: cold and warm EXPLAINs must stay identical both without
-- stats (fallback path) and with stats (trusted path).
--
-- Settings pinned at session and inline level because CI injects random values
-- for `query_plan_join_swap_table`, `query_plan_optimize_join_order_limit`,
-- `query_plan_optimize_join_order_algorithm`, `enable_join_runtime_filters`,
-- `use_statistics`, and `query_plan_read_in_order_through_join`.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 'auto';
SET query_plan_read_in_order_through_join = 0;
SET enable_join_runtime_filters = 1;
SET use_hash_table_stats_for_join_reordering = 1;
SET collect_hash_table_stats_during_joins = 1;

DROP TABLE IF EXISTS t_fact_04105;
DROP TABLE IF EXISTS t_dim_04105;

CREATE TABLE t_fact_04105 (k UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_dim_04105  (k UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_fact_04105 SELECT number % 100 FROM numbers(10000);
INSERT INTO t_dim_04105  SELECT number       FROM numbers(100);

-- The subquery's `WHERE k < 10` forces filtered `source_rows` into the cache.
-- Inline SETTINGS reset unlisted values to CLI defaults, so repeat the
-- plan-affecting ones here.

SELECT '-- use_statistics = 0: cold EXPLAIN (cache empty)';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_fact_04105 f JOIN (SELECT k FROM t_dim_04105 WHERE k < 10) d ON f.k = d.k
    SETTINGS use_statistics = 0, query_plan_join_swap_table = 'auto',
             query_plan_optimize_join_order_limit = 10,
             query_plan_optimize_join_order_algorithm = 'greedy',
             query_plan_read_in_order_through_join = 0,
             enable_join_runtime_filters = 1,
             use_hash_table_stats_for_join_reordering = 1,
             collect_hash_table_stats_during_joins = 1
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

SELECT count() FROM t_fact_04105 f JOIN (SELECT k FROM t_dim_04105 WHERE k < 10) d ON f.k = d.k
SETTINGS use_statistics = 0, query_plan_join_swap_table = 'auto',
         query_plan_optimize_join_order_limit = 10,
         query_plan_optimize_join_order_algorithm = 'greedy',
         query_plan_read_in_order_through_join = 0,
         enable_join_runtime_filters = 1,
         use_hash_table_stats_for_join_reordering = 1,
         collect_hash_table_stats_during_joins = 1
FORMAT Null;

SELECT '-- use_statistics = 0: warm EXPLAIN (must match cold)';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_fact_04105 f JOIN (SELECT k FROM t_dim_04105 WHERE k < 10) d ON f.k = d.k
    SETTINGS use_statistics = 0, query_plan_join_swap_table = 'auto',
             query_plan_optimize_join_order_limit = 10,
             query_plan_optimize_join_order_algorithm = 'greedy',
             query_plan_read_in_order_through_join = 0,
             enable_join_runtime_filters = 1,
             use_hash_table_stats_for_join_reordering = 1,
             collect_hash_table_stats_during_joins = 1
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

SELECT '-- use_statistics = 1: cold EXPLAIN (trusted estimates, numeric ResultRows)';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_fact_04105 f JOIN (SELECT k FROM t_dim_04105 WHERE k < 10) d ON f.k = d.k
    SETTINGS use_statistics = 1, query_plan_join_swap_table = 'auto',
             query_plan_optimize_join_order_limit = 10,
             query_plan_optimize_join_order_algorithm = 'greedy',
             query_plan_read_in_order_through_join = 0,
             enable_join_runtime_filters = 1,
             use_hash_table_stats_for_join_reordering = 1,
             collect_hash_table_stats_during_joins = 1
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

SELECT count() FROM t_fact_04105 f JOIN (SELECT k FROM t_dim_04105 WHERE k < 10) d ON f.k = d.k
SETTINGS use_statistics = 1, query_plan_join_swap_table = 'auto',
         query_plan_optimize_join_order_limit = 10,
         query_plan_optimize_join_order_algorithm = 'greedy',
         query_plan_read_in_order_through_join = 0,
         enable_join_runtime_filters = 1,
         use_hash_table_stats_for_join_reordering = 1,
         collect_hash_table_stats_during_joins = 1
FORMAT Null;

SELECT '-- use_statistics = 1: warm EXPLAIN (must match cold)';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_fact_04105 f JOIN (SELECT k FROM t_dim_04105 WHERE k < 10) d ON f.k = d.k
    SETTINGS use_statistics = 1, query_plan_join_swap_table = 'auto',
             query_plan_optimize_join_order_limit = 10,
             query_plan_optimize_join_order_algorithm = 'greedy',
             query_plan_read_in_order_through_join = 0,
             enable_join_runtime_filters = 1,
             use_hash_table_stats_for_join_reordering = 1,
             collect_hash_table_stats_during_joins = 1
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_fact_04105;
DROP TABLE t_dim_04105;
