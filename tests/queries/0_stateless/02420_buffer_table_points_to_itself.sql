-- Tags: no-parallel
-- no-parallel to avoid seeing INFINITE_LOOP in other tests

-- Test for total_rows with recursive buffer.
DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt32) Engine=Buffer(default, test, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
SELECT * FROM system.tables WHERE database = currentDatabase() AND table = 'test' FORMAT Null; -- { serverError INFINITE_LOOP }
