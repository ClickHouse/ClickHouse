DROP TABLE IF EXISTS test_table_for_01070_exception_code_in_query_log_table;
SELECT * FROM test_table_for_01070_exception_code_in_query_log_table; -- { serverError 60 }
CREATE TABLE test_table_for_01070_exception_code_in_query_log_table (value UInt64) ENGINE=Memory();
SELECT * FROM test_table_for_01070_exception_code_in_query_log_table;
SYSTEM FLUSH LOGS;
SELECT exception_code FROM system.query_log WHERE query = 'SELECT * FROM test_table_for_01070_exception_code_in_query_log_table' ORDER BY exception_code;
DROP TABLE IF EXISTS test_table_for_01070_exception_code_in_query_log_table;
