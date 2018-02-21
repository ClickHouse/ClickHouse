DROP TABLE IF EXISTS test.nested;

CREATE TABLE test.nested (x UInt64, filter UInt8, n Nested(a UInt64)) ENGINE = MergeTree ORDER BY x;
INSERT INTO test.nested SELECT number, number % 2, range(number % 10) FROM system.numbers LIMIT 100000;

ALTER TABLE test.nested ADD COLUMN n.b Array(UInt64);
SELECT DISTINCT n.b FROM test.nested PREWHERE filter;

DROP TABLE test.nested;
