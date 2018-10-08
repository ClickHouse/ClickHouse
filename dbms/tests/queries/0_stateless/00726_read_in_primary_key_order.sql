DROP TABLE IF EXISTS test.merge1;

CREATE TABLE test.merge1(a Date, b UInt64) ENGINE=MergeTree() PARTITION BY a ORDER BY b;

INSERT INTO test.merge1 VALUES (1, 1), (1, 2), (1, 3), (100, 1), (100, 2), (200, 1), (200, 4);

SELECT b FROM test.merge1 ORDER BY b;
SELECT b FROM test.merge1 ORDER BY b DESC;

DROP TABLE IF EXISTS test.merge;
