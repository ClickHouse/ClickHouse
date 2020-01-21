DROP TABLE IF EXISTS test_table;
SYSTEM FLUSH LOGS;
TRUNCATE TABLE system.query_log;
SELECT * FROM test_table; -- { serverError 60 }
SYSTEM FLUSH LOGS;
SELECT exception_code FROM system.query_log;
