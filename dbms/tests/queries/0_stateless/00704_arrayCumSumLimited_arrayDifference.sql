DROP TABLE IF EXISTS test.test;

SELECT arrayCumSumLimited([1, 2, 3, 4]);

SELECT arrayCumSumLimited([1, -5, 5, -2]);

SELECT arrayDifference([1, 2, 3, 4]);

SELECT arrayDifference([1, 7, 100, 5]);

CREATE TABLE test.test(a Array(Int64), b Array(Float64), c Array(UInt64)) ENGINE=Memory;

INSERT INTO test.test VALUES ([1, -3, 0, 1], [1.0, 0.4, -0.1], [1, 3, 1]);

SELECT arrayCumSumLimited(a) FROM test.test;

SELECT arrayCumSumLimited(b) FROM test.test;

SELECT arrayCumSumLimited(c) FROM test.test;

SELECT arrayDifference(a) FROM test.test;

SELECT arrayDifference(b) FROM test.test;

SELECT arrayDifference(c) FROM test.test;

DROP TABLE IF EXISTS test.test;

