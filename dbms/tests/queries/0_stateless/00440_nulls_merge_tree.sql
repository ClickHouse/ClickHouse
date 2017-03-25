DROP TABLE IF EXISTS test.nulls;
CREATE TABLE test.nulls (d Date, x Nullable(UInt64)) ENGINE = MergeTree(d, d, 8192);
INSERT INTO test.nulls SELECT toDate('2000-01-01'), number % 10 != 0 ? number : NULL FROM system.numbers LIMIT 10000;
SELECT count() FROM test.nulls WHERE x IS NULL;
DROP TABLE test.nulls;
