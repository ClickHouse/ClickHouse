-- Test that ANY INNER JOIN with the partial_merge algorithm returns one row per matched
-- left key regardless of the right block size, i.e. that the result does not depend on how
-- right rows of one key are split across right blocks (partial_merge_join_rows_in_right_blocks).
-- See https://github.com/ClickHouse/ClickHouse/pull/104621

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (id Int32, key String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t2 (id Int32, val String) ENGINE = MergeTree ORDER BY id;

-- id = 2 and id = 3 have several rows on both sides, so a single key spans multiple right blocks
-- once the right blocks are split.
INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (2, 'c'), (3, 'd'), (3, 'e'), (4, 'f');
INSERT INTO t2 VALUES (2, 'x'), (2, 'y'), (2, 'z'), (3, 'p'), (3, 'q'), (5, 'r');

SELECT '-- hash (reference) --';
SELECT t1.id FROM t1 INNER ANY JOIN t2 ON t1.id = t2.id ORDER BY t1.id SETTINGS join_algorithm = 'hash';

SELECT '-- partial_merge --';
SELECT t1.id FROM t1 INNER ANY JOIN t2 ON t1.id = t2.id ORDER BY t1.id SETTINGS join_algorithm = 'partial_merge';

SELECT '-- partial_merge, right blocks split = 1 --';
SELECT t1.id FROM t1 INNER ANY JOIN t2 ON t1.id = t2.id ORDER BY t1.id SETTINGS join_algorithm = 'partial_merge', partial_merge_join_rows_in_right_blocks = 1;

SELECT '-- partial_merge, right blocks split = 2 --';
SELECT t1.id FROM t1 INNER ANY JOIN t2 ON t1.id = t2.id ORDER BY t1.id SETTINGS join_algorithm = 'partial_merge', partial_merge_join_rows_in_right_blocks = 2;

DROP TABLE t1;
DROP TABLE t2;
