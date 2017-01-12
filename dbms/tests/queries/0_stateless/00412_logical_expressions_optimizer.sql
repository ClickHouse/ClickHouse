DROP TABLE IF EXISTS test.merge_tree;
CREATE TABLE test.merge_tree (x UInt64, date Date) ENGINE = MergeTree(date, x, 1);

INSERT INTO test.merge_tree VALUES (1, '2000-01-01');
SELECT x AS y, y FROM test.merge_tree;

DROP TABLE IF EXISTS test.merge_tree;
