SELECT '*** MergeTree ***';

DROP TABLE IF EXISTS unsorted;
CREATE TABLE unsorted (x UInt32, y String) ENGINE MergeTree ORDER BY tuple() SETTINGS vertical_merge_algorithm_min_rows_to_activate=0, vertical_merge_algorithm_min_columns_to_activate=0;

INSERT INTO unsorted VALUES (1, 'a'), (5, 'b');
INSERT INTO unsorted VALUES (2, 'c'), (4, 'd');
INSERT INTO unsorted VALUES (3, 'e');

OPTIMIZE TABLE unsorted PARTITION tuple() FINAL;

SELECT * FROM unsorted;

DROP TABLE unsorted;

SET allow_suspicious_primary_key = 1;
SELECT '*** ReplacingMergeTree ***';

DROP TABLE IF EXISTS unsorted_replacing;

CREATE TABLE unsorted_replacing (x UInt32, s String, v UInt32) ENGINE ReplacingMergeTree(v) ORDER BY tuple();

INSERT INTO unsorted_replacing VALUES (1, 'a', 5), (5, 'b', 4);
INSERT INTO unsorted_replacing VALUES (2, 'c', 3), (4, 'd', 2);
INSERT INTO unsorted_replacing VALUES (3, 'e', 1);

SELECT * FROM unsorted_replacing FINAL;

SELECT '---';

OPTIMIZE TABLE unsorted_replacing PARTITION tuple() FINAL;

SELECT * FROM unsorted_replacing;

DROP TABLE unsorted_replacing;


SELECT '*** CollapsingMergeTree ***';

DROP TABLE IF EXISTS unsorted_collapsing;

CREATE TABLE unsorted_collapsing (x UInt32, s String, sign Int8) ENGINE CollapsingMergeTree(sign) ORDER BY tuple();

INSERT INTO unsorted_collapsing VALUES (1, 'a', 1);
INSERT INTO unsorted_collapsing VALUES (1, 'a', -1), (2, 'b', 1);
INSERT INTO unsorted_collapsing VALUES (2, 'b', -1), (3, 'c', 1);

SELECT * FROM unsorted_collapsing FINAL;

SELECT '---';

OPTIMIZE TABLE unsorted_collapsing PARTITION tuple() FINAL;

SELECT * FROM unsorted_collapsing;

DROP TABLE unsorted_collapsing;
