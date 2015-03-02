DROP TABLE IF EXISTS test.mt;
DROP TABLE IF EXISTS test.buf;

CREATE TABLE test.mt (a UInt8, d Date) ENGINE = MergeTree(d, a, 1);
CREATE TABLE test.buf AS test.mt ENGINE = Buffer(test, mt, 1, 100, 100, 1000000, 1000000, 1000000000, 1000000000);

INSERT INTO test.buf SELECT toUInt8(number) AS a, toDate('2015-01-01') AS d FROM system.numbers LIMIT 1000;
SELECT count() FROM test.mt;
SELECT count() FROM test.buf;
SELECT * FROM test.buf PREWHERE a < 10;

OPTIMIZE TABLE test.buf;
SELECT count() FROM test.mt;
SELECT count() FROM test.buf;
SELECT * FROM test.buf PREWHERE a < 10;

DROP TABLE test.buf;
DROP TABLE test.mt;
