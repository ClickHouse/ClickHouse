DROP TABLE IF EXISTS test.join_test;
CREATE TABLE test.join_test (number UInt8, value Float32) Engine = Join(ANY, LEFT, number);
TRUNCATE TABLE test.join_test;
DROP TABLE IF EXISTS test.join_test;
