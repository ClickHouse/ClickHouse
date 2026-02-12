-- Tags: no-parallel
-- Test RowsAfterPrewhereAndWhereFilter profile event

DROP TABLE IF EXISTS test_output_rows;
CREATE TABLE test_output_rows (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO test_output_rows SELECT number, toString(number) FROM numbers(1000);

SYSTEM FLUSH LOGS;

-- Case 1: No WHERE -- counts all storage output rows
SELECT * FROM test_output_rows FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RowsAfterPrewhereAndWhereFilter']
FROM system.query_log
WHERE (type = 'QueryFinish') AND (current_database = currentDatabase())
    AND (query LIKE '%Case 1%FROM test_output_rows FORMAT Null%')
    AND (query NOT LIKE '%system.query_log%');

-- Case 2: WHERE fully pushed to PREWHERE (simple column filter) -- counts post-all-filtering rows
SELECT * FROM test_output_rows WHERE k < 100 FORMAT Null; -- Case 2
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RowsAfterPrewhereAndWhereFilter']
FROM system.query_log
WHERE (type = 'QueryFinish') AND (current_database = currentDatabase())
    AND (query LIKE '%Case 2%FROM test_output_rows WHERE%FORMAT Null%')
    AND (query NOT LIKE '%system.query_log%');

-- Case 3: WHERE NOT pushed to PREWHERE (non-deterministic function prevents push)
-- optimize_move_to_prewhere=0 forces WHERE to stay as FilterTransform
SELECT * FROM test_output_rows WHERE k < 100 FORMAT Null SETTINGS optimize_move_to_prewhere=0; -- Case 3
SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['RowsAfterPrewhereAndWhereFilter']
FROM system.query_log
WHERE (type = 'QueryFinish') AND (current_database = currentDatabase())
    AND (query LIKE '%Case 3%FROM test_output_rows WHERE%FORMAT Null%')
    AND (query NOT LIKE '%system.query_log%');

DROP TABLE test_output_rows;
