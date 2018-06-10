SELECT '*** MergeTree ***';

DROP TABLE IF EXISTS test.unsorted;
CREATE TABLE test.unsorted (x UInt32, y String) ENGINE MergeTree ORDER BY tuple() SETTINGS vertical_merge_algorithm_min_rows_to_activate=0, vertical_merge_algorithm_min_columns_to_activate=0;

INSERT INTO test.unsorted VALUES (1, 'a'), (5, 'b');
INSERT INTO test.unsorted VALUES (2, 'c'), (4, 'd');
INSERT INTO test.unsorted VALUES (3, 'e');

OPTIMIZE TABLE test.unsorted PARTITION tuple() FINAL;

SELECT * FROM test.unsorted;

DROP TABLE test.unsorted;


SELECT '*** ReplacingMergeTree ***';

DROP TABLE IF EXISTS test.unsorted_replacing;

CREATE TABLE test.unsorted_replacing (x UInt32, s String, v UInt32) ENGINE ReplacingMergeTree(v) ORDER BY tuple();

INSERT INTO test.unsorted_replacing VALUES (1, 'a', 5), (5, 'b', 4);
INSERT INTO test.unsorted_replacing VALUES (2, 'c', 3), (4, 'd', 2);
INSERT INTO test.unsorted_replacing VALUES (3, 'e', 1);

SELECT * FROM test.unsorted_replacing FINAL;

SELECT '---';

OPTIMIZE TABLE test.unsorted_replacing PARTITION tuple() FINAL;

SELECT * FROM test.unsorted_replacing;

DROP TABLE test.unsorted_replacing;


SELECT '*** CollapsingMergeTree ***';

DROP TABLE IF EXISTS test.unsorted_collapsing;

CREATE TABLE test.unsorted_collapsing (x UInt32, s String, sign Int8) ENGINE CollapsingMergeTree(sign) ORDER BY tuple();

INSERT INTO test.unsorted_collapsing VALUES (1, 'a', 1);
INSERT INTO test.unsorted_collapsing VALUES (1, 'a', -1), (2, 'b', 1);
INSERT INTO test.unsorted_collapsing VALUES (2, 'b', -1), (3, 'c', 1);

SELECT * FROM test.unsorted_collapsing FINAL;

SELECT '---';

OPTIMIZE TABLE test.unsorted_collapsing PARTITION tuple() FINAL;

SELECT * FROM test.unsorted_collapsing;

DROP TABLE test.unsorted_collapsing;
