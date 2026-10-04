-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- `groupArray(v)[1]` keeps the sort in the plan; `count()` would be answered from a projection.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_sort_limits_subquery;

CREATE TABLE t_sort_limits_subquery (k UInt64, v UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_sort_limits_subquery SELECT number, number * 2 FROM numbers(100000);

SELECT '-- A subquery limit is enforced without Cascades';
SELECT (SELECT groupArray(v)[1] FROM (SELECT v FROM t_sort_limits_subquery ORDER BY v)
        SETTINGS max_rows_to_sort = 10, sort_overflow_mode = 'throw'); -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT '-- A top-level limit is enforced under Cascades';
SELECT (SELECT groupArray(v)[1] FROM (SELECT v FROM t_sort_limits_subquery ORDER BY v))
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    max_rows_to_sort = 10, sort_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT '-- A subquery limit is enforced under Cascades while the enclosing query is permissive';
SELECT (SELECT groupArray(v)[1] FROM (SELECT v FROM t_sort_limits_subquery ORDER BY v)
        SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
            max_rows_to_sort = 10, sort_overflow_mode = 'throw'); -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT '-- Limits on both levels are enforced under Cascades';
SELECT (SELECT groupArray(v)[1] FROM (SELECT v FROM t_sort_limits_subquery ORDER BY v)
        SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
            max_rows_to_sort = 10, sort_overflow_mode = 'throw')
SETTINGS max_rows_to_sort = 10, sort_overflow_mode = 'throw'; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT '-- A subquery raising the limit above the enclosing query succeeds under Cascades';
SELECT (SELECT groupArray(v)[1] FROM (SELECT v FROM t_sort_limits_subquery ORDER BY v)
        SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
            max_rows_to_sort = 1000000, sort_overflow_mode = 'break')
SETTINGS max_rows_to_sort = 10, sort_overflow_mode = 'throw';

SELECT '-- Same query without Cascades';
SELECT (SELECT groupArray(v)[1] FROM (SELECT v FROM t_sort_limits_subquery ORDER BY v)
        SETTINGS max_rows_to_sort = 1000000, sort_overflow_mode = 'break')
SETTINGS max_rows_to_sort = 10, sort_overflow_mode = 'throw';

-- The limit is reached in both scopes, so the sort consults the overflow mode, and only the mode
-- differs between them. The value is the first row of an ascending sort, which survives truncation
-- whatever the block size.
SELECT '-- A subquery relaxing only the overflow mode returns rather than throwing under Cascades';
SELECT (SELECT groupArray(v)[1] FROM (SELECT v FROM t_sort_limits_subquery ORDER BY v)
        SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
            max_rows_to_sort = 10, sort_overflow_mode = 'break')
SETTINGS max_rows_to_sort = 10, sort_overflow_mode = 'throw';

SELECT '-- Same relaxed-mode query without Cascades';
SELECT (SELECT groupArray(v)[1] FROM (SELECT v FROM t_sort_limits_subquery ORDER BY v)
        SETTINGS max_rows_to_sort = 10, sort_overflow_mode = 'break')
SETTINGS max_rows_to_sort = 10, sort_overflow_mode = 'throw';

SELECT '-- An out-of-range spill ratio is still rejected only where a sort is planned';
SELECT 1 SETTINGS max_bytes_ratio_before_external_sort = 1.5;
SELECT 1 SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    max_bytes_ratio_before_external_sort = 1.5; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_sort_limits_subquery;
