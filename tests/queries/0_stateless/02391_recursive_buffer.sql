-- Tags: no-parallel
-- because of system.tables poisoning

DROP TABLE IF EXISTS test;
CREATE TABLE test (key UInt32) Engine = Buffer(currentDatabase(), test, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
SELECT * FROM test; -- { serverError 269 }
SELECT * FROM system.tables WHERE table = 'test' AND database = currentDatabase() FORMAT Null; -- { serverError 269 }
DROP TABLE test;

DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;
CREATE TABLE test1 (key UInt32) Engine = Buffer(currentDatabase(), test2, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
CREATE TABLE test2 (key UInt32) Engine = Buffer(currentDatabase(), test1, 16, 10, 100, 10000, 1000000, 10000000, 100000000);
SELECT * FROM test1; -- { serverError 306 }
SELECT * FROM test2; -- { serverError 306 }
SELECT * FROM system.tables WHERE table IN ('test1', 'test2') AND database = currentDatabase(); -- { serverError 306 }
DROP TABLE test1;
DROP TABLE test2;
