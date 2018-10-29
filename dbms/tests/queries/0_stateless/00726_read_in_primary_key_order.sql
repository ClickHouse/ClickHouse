CREATE DATABASE IF NOT EXISTS test;

DROP TABLE IF EXISTS test.merge1;

CREATE TABLE test.merge1(a Date, b UInt64) ENGINE=MergeTree(a, b ,10);

INSERT INTO test.merge1 VALUES (1, 1), (1, 2), (1, 3);
INSERT INTO test.merge1 VALUES (100, 4), (100, 5);
INSERT INTO test.merge1 VALUES (200, 6), (200, 7);
INSERT INTO test.merge1 VALUES (300, 8), (300, 9), (300, 0);

SELECT b FROM test.merge1 ORDER BY b;
SELECT b FROM test.merge1 ORDER BY b DESC;

SET max_threads=1;

SELECT b FROM test.merge1 ORDER BY b;
SELECT b FROM test.merge1 ORDER BY b DESC;

DROP TABLE IF EXISTS test.merge1;

SET max_threads=8;
SET merge_tree_min_rows_for_concurrent_read = 1;

CREATE TABLE test.merge1(a Date, b UInt64) ENGINE=MergeTree(a, b, 10);

SET max_insert_block_size=8192;

INSERT INTO test.merge1 SELECT 1, rand() FROM system.numbers LIMIT 100000;
SELECT min(runningDifference(b)) FROM test.merge1;

DROP TABLE IF EXISTS test.merge1;
