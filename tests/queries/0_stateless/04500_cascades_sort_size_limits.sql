-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- A sort built by the Cascades SortingEnforcer must keep the query's sort settings
-- (size limits, spill thresholds). A sort built with default settings would silently
-- ignore `max_rows_to_sort`.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_sort_limits;

CREATE TABLE t_sort_limits (k UInt64, v UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_sort_limits SELECT number, number * 2 FROM numbers(100000);

SELECT '-- max_rows_to_sort is enforced under Cascades';
SELECT * FROM t_sort_limits ORDER BY v FORMAT Null
SETTINGS max_rows_to_sort = 10, sort_overflow_mode = 'throw', distributed_plan_execute_locally = 1; -- { serverError TOO_MANY_ROWS_OR_BYTES }

SELECT '-- Baseline without Cascades fails the same way';
SELECT * FROM t_sort_limits ORDER BY v FORMAT Null
SETTINGS max_rows_to_sort = 10, sort_overflow_mode = 'throw',
    enable_cascades_optimizer = 0, make_distributed_plan = 0; -- { serverError TOO_MANY_ROWS_OR_BYTES }

DROP TABLE t_sort_limits;
