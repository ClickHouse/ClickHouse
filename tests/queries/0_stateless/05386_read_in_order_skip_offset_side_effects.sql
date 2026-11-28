-- Tags: no-random-merge-tree-settings

-- A function with observable side effects (`sleepEachRow`) must be evaluated on as many rows with the OFFSET-skip
-- read-in-order optimization as without it, so the optimization must not drop the leading granules below it.

DROP TABLE IF EXISTS t_skip_offset_side_effects;
CREATE TABLE t_skip_offset_side_effects (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_skip_offset_side_effects SELECT number FROM numbers(10);

-- The function is evaluated before the sorting, below the offset.
SET optimize_read_in_order = 1, max_threads = 1, query_plan_execute_functions_after_sorting = 0, enable_parallel_replicas = 0;

SELECT k, sleepEachRow(0) FROM t_skip_offset_side_effects ORDER BY k LIMIT 2 OFFSET 3
SETTINGS log_comment = '05386_sleep_on', query_plan_optimize_read_in_order_skip_offset = 1;
SELECT k, sleepEachRow(0) FROM t_skip_offset_side_effects ORDER BY k LIMIT 2 OFFSET 3
SETTINGS log_comment = '05386_sleep_off', query_plan_optimize_read_in_order_skip_offset = 0;

-- Without the function, the leading granules are skipped.
SELECT k FROM t_skip_offset_side_effects ORDER BY k LIMIT 2 OFFSET 3
SETTINGS log_comment = '05386_plain_on', query_plan_optimize_read_in_order_skip_offset = 1;
SELECT k FROM t_skip_offset_side_effects ORDER BY k LIMIT 2 OFFSET 3
SETTINGS log_comment = '05386_plain_off', query_plan_optimize_read_in_order_skip_offset = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    sumIf(read_rows, log_comment = '05386_sleep_on') = sumIf(read_rows, log_comment = '05386_sleep_off') AS sleep_reads_all,
    sumIf(read_rows, log_comment = '05386_plain_on') < sumIf(read_rows, log_comment = '05386_plain_off') AS plain_skips
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05386_sleep_on', '05386_sleep_off', '05386_plain_on', '05386_plain_off');

DROP TABLE t_skip_offset_side_effects;
