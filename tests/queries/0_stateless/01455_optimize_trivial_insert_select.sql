-- Tags: log-engine
SET max_insert_threads = 1, max_threads = 100, min_insert_block_size_rows = 1048576, max_block_size = 65536;
SET allow_deprecated_error_prone_window_functions = 1;
DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt64) ENGINE = StripeLog;
-- For trivial INSERT SELECT, max_threads is lowered to max_insert_threads and max_block_size is changed to min_insert_block_size_rows.
SET optimize_trivial_insert_select = 1;
INSERT INTO t SELECT * FROM numbers_mt(1000000);
SET max_threads = 1;
-- If data was inserted by more threads, we will probably see data out of order.
SELECT DISTINCT blockSize(), runningDifference(x) FROM t;
DROP TABLE t;
