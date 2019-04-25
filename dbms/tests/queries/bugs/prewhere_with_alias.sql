DROP TABLE IF EXISTS test.test;
CREATE TABLE test.test (a UInt8, b UInt8, c UInt16 ALIAS a + b) ENGINE = MergeTree ORDER BY a;

SELECT b FROM test.test PREWHERE c = 1;

DROP TABLE test;
