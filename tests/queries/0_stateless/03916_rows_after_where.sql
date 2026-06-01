-- Tags: no-random-settings, no-random-merge-tree-settings

DROP TABLE IF EXISTS test_03916_output_rows;
CREATE TABLE test_03916_output_rows (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO test_03916_output_rows SELECT number, toString(number) FROM numbers(1000);

CREATE TEMPORARY TABLE start_ts AS SELECT now() AS ts;

-- Case 1: no `WHERE` -- counts all storage output rows
SELECT * FROM test_03916_output_rows FORMAT Null SETTINGS log_comment = '03916_case1';
-- Case 2: `WHERE` fully pushed to `PREWHERE` (simple column filter)
SELECT * FROM test_03916_output_rows WHERE k < 100 FORMAT Null SETTINGS log_comment = '03916_case2';
-- Case 3: `WHERE` not pushed to `PREWHERE`
SELECT * FROM test_03916_output_rows WHERE k < 100 FORMAT Null SETTINGS optimize_move_to_prewhere = 0, log_comment = '03916_case3';
-- Case 4: `WHERE` not pushed to `PREWHERE` and `query_plan_optimize_prewhere` disabled
SELECT * FROM test_03916_output_rows WHERE k < 100 FORMAT Null SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0, log_comment = '03916_case4';
-- Case 5: `WHERE` filter survives lazy materialization rewrite
SELECT v FROM test_03916_output_rows WHERE k < 100 ORDER BY k LIMIT 10 FORMAT Null
SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 1000, log_comment = '03916_case5';

SYSTEM FLUSH LOGS query_log;

-- `RowsAfterWhere` should be consistent regardless of push-down
SELECT ProfileEvents['RowsAfterWhere']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case1' AND event_time >= (SELECT ts FROM start_ts)
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT ProfileEvents['RowsAfterWhere']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case2' AND event_time >= (SELECT ts FROM start_ts)
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT ProfileEvents['RowsAfterWhere']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case3' AND event_time >= (SELECT ts FROM start_ts)
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT ProfileEvents['RowsAfterWhere']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case4' AND event_time >= (SELECT ts FROM start_ts)
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT ProfileEvents['RowsAfterWhere']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case5' AND event_time >= (SELECT ts FROM start_ts)
ORDER BY event_time_microseconds DESC LIMIT 1;

-- Sanity: verify Case 2 pushed `WHERE` to `PREWHERE` (no `FilterTransform`), other cases did not
SELECT ProfileEvents['FilterTransformPassedRows']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case2' AND event_time >= (SELECT ts FROM start_ts)
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT ProfileEvents['FilterTransformPassedRows']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case3' AND event_time >= (SELECT ts FROM start_ts)
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT ProfileEvents['FilterTransformPassedRows']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case4' AND event_time >= (SELECT ts FROM start_ts)
ORDER BY event_time_microseconds DESC LIMIT 1;

SELECT ProfileEvents['FilterTransformPassedRows']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case5' AND event_time >= (SELECT ts FROM start_ts)
ORDER BY event_time_microseconds DESC LIMIT 1;

DROP TABLE test_03916_output_rows;
