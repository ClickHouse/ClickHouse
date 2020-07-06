DROP TABLE IF EXISTS test_01307;
CREATE TABLE test_01307 (id UInt64, val String, INDEX ind val TYPE bloom_filter() GRANULARITY 1) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 2;
INSERT INTO test_01307 (id, val) select number as id, toString(number) as val from numbers(4);
SELECT count() FROM test_01307 WHERE identity(val) = '2';
SELECT count() FROM test_01307 WHERE val = '2';
OPTIMIZE TABLE test_01307 FINAL;
SELECT count() FROM test_01307 WHERE identity(val) = '2';
SELECT count() FROM test_01307 WHERE val = '2';
