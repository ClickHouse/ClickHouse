-- This test creates many threads to test a case when ThreadPool will remove some threads from pool after job is done.
SET max_block_size = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0;

USE test;

CREATE TEMPORARY TABLE t (x UInt64);
INSERT INTO t SELECT * FROM system.numbers LIMIT 1500;

SELECT DISTINCT blockSize() FROM t;

SET max_threads = 1500;

SELECT count() FROM t;
SELECT sum(sleep(0.1)) FROM t; -- All threads have time to be created.
SELECT 'Ok.';
