-- Tags: no-parallel

DROP TABLE IF EXISTS test_output_rows;
CREATE TABLE test_output_rows (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO test_output_rows SELECT number, toString(number) FROM numbers(1000);

-- Case 1: No WHERE -- counts all storage output rows
SELECT * FROM test_output_rows FORMAT Null SETTINGS log_comment = '03916_case1';
-- Case 2: WHERE fully pushed to PREWHERE (simple column filter)
SELECT * FROM test_output_rows WHERE k < 100 FORMAT Null SETTINGS log_comment = '03916_case2';
-- Case 3: WHERE NOT pushed to PREWHERE
SELECT * FROM test_output_rows WHERE k < 100 FORMAT Null SETTINGS optimize_move_to_prewhere = 0, log_comment = '03916_case3';

SYSTEM FLUSH LOGS query_log;

-- RowsAfterWhere should be consistent regardless of push-down
SELECT ProfileEvents['RowsAfterWhere']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case1';

SELECT ProfileEvents['RowsAfterWhere']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case2';

SELECT ProfileEvents['RowsAfterWhere']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case3';

-- Sanity: verify Case 2 pushed WHERE to PREWHERE (no FilterTransform), Case 3 did not (FilterTransform active)
SELECT ProfileEvents['FilterTransformPassedRows']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case2';

SELECT ProfileEvents['FilterTransformPassedRows']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '03916_case3';

DROP TABLE test_output_rows;
