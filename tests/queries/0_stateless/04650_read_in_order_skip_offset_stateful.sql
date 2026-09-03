-- Tags: no-random-merge-tree-settings

-- A stateful function evaluated below the OFFSET (`rowNumberInAllBlocks`, `neighbor`, `runningDifference`)
-- computes its result for a row from the rows preceding it, so the OFFSET-skip read-in-order optimization must
-- not drop leading granules below such an expression - the kept rows would then be evaluated with different
-- state. Every query below must return the same values with the optimization enabled and disabled.

DROP TABLE IF EXISTS t_skip_offset_stateful;
CREATE TABLE t_skip_offset_stateful (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_skip_offset_stateful SELECT number FROM numbers(10);

SET allow_deprecated_error_prone_window_functions = 1, optimize_read_in_order = 1, max_threads = 1;

SELECT 'runningDifference under LIMIT/OFFSET, disabled';
SELECT k, runningDifference(k) AS d FROM t_skip_offset_stateful ORDER BY k LIMIT 2 OFFSET 3
SETTINGS query_plan_optimize_read_in_order_skip_offset = 0;
SELECT 'runningDifference under LIMIT/OFFSET, enabled';
SELECT k, runningDifference(k) AS d FROM t_skip_offset_stateful ORDER BY k LIMIT 2 OFFSET 3
SETTINGS query_plan_optimize_read_in_order_skip_offset = 1;

SELECT 'neighbor under LIMIT/OFFSET, disabled';
SELECT k, neighbor(k, -1) AS p FROM t_skip_offset_stateful ORDER BY k LIMIT 2 OFFSET 3
SETTINGS query_plan_optimize_read_in_order_skip_offset = 0;
SELECT 'neighbor under LIMIT/OFFSET, enabled';
SELECT k, neighbor(k, -1) AS p FROM t_skip_offset_stateful ORDER BY k LIMIT 2 OFFSET 3
SETTINGS query_plan_optimize_read_in_order_skip_offset = 1;

SELECT 'rowNumberInAllBlocks under a pure OFFSET, disabled';
SELECT k, n FROM (SELECT k, rowNumberInAllBlocks() AS n FROM t_skip_offset_stateful ORDER BY k) OFFSET 3
SETTINGS query_plan_optimize_read_in_order_skip_offset = 0;
SELECT 'rowNumberInAllBlocks under a pure OFFSET, enabled';
SELECT k, n FROM (SELECT k, rowNumberInAllBlocks() AS n FROM t_skip_offset_stateful ORDER BY k) OFFSET 3
SETTINGS query_plan_optimize_read_in_order_skip_offset = 1;

DROP TABLE t_skip_offset_stateful;
