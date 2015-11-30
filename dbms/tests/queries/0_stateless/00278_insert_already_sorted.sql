DROP TABLE IF EXISTS test.sorted;
CREATE TABLE test.sorted (d Date DEFAULT '2000-01-01', x UInt64) ENGINE = MergeTree(d, x, 8192);

INSERT INTO test.sorted (x) SELECT intDiv(number, 100000) AS x FROM system.numbers LIMIT 1000000;

SET max_threads = 1;

SELECT count() FROM test.sorted;
SELECT DISTINCT x FROM test.sorted;

INSERT INTO test.sorted (x) SELECT (intHash64(number) % 1000 = 0 ? 999 : intDiv(number, 100000)) AS x FROM system.numbers LIMIT 1000000;

SELECT count() FROM test.sorted;
SELECT DISTINCT x FROM test.sorted;

DROP TABLE test.sorted;
