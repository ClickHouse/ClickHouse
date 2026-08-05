-- Tags: no-random-settings
-- Fully-matched single-disjunct RIGHT/FULL must emit zero unmatched right rows, leftover right keys
-- must still appear, and nullable right keys must still emit nullmap rows. Covers serial (`hash`)
-- and parallel (`parallel_hash`, threshold 0) layouts.

SET join_use_nulls = 1;
SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

DROP TABLE IF EXISTS t_l;
DROP TABLE IF EXISTS t_r_matched;
DROP TABLE IF EXISTS t_r_extra;
DROP TABLE IF EXISTS t_l_null;
DROP TABLE IF EXISTS t_r_null;

CREATE TABLE t_l (id UInt32, v String) ENGINE = Memory;
CREATE TABLE t_r_matched (id UInt32, d String) ENGINE = Memory;
CREATE TABLE t_r_extra (id UInt32, d String) ENGINE = Memory;
INSERT INTO t_l VALUES (1, 'A'), (2, 'B'), (3, 'C');
INSERT INTO t_r_matched VALUES (1, 'one'), (2, 'two');
INSERT INTO t_r_extra VALUES (1, 'one'), (2, 'two'), (9, 'nine');

CREATE TABLE t_l_null (id UInt32, v String) ENGINE = Memory;
CREATE TABLE t_r_null (id Nullable(UInt32), d String) ENGINE = Memory;
INSERT INTO t_l_null VALUES (1, 'A'), (2, 'B');
INSERT INTO t_r_null VALUES (1, 'one'), (NULL, 'null1'), (2, 'two');

SET join_algorithm = 'hash';
SET parallel_hash_join_threshold = 100000;

SELECT 'hash_right_all_matched';
SELECT count(), countIf(l.id IS NULL)
FROM t_l AS l
RIGHT JOIN t_r_matched AS r ON l.id = r.id;

SELECT 'hash_full_all_matched';
SELECT count(), countIf(l.id IS NULL), countIf(r.id IS NULL)
FROM t_l AS l
FULL OUTER JOIN t_r_matched AS r ON l.id = r.id;

SELECT 'hash_right_nullmap';
SELECT count(), countIf(l.id IS NULL)
FROM t_l_null AS l
RIGHT JOIN t_r_null AS r ON l.id = r.id;

SELECT 'hash_right_unmatched';
SELECT count(), countIf(l.id IS NULL)
FROM t_l AS l
RIGHT JOIN t_r_extra AS r ON l.id = r.id;

SELECT 'hash_full_unmatched';
SELECT count(), countIf(l.id IS NULL), countIf(r.id IS NULL)
FROM t_l AS l
FULL OUTER JOIN t_r_extra AS r ON l.id = r.id;

SET join_algorithm = 'parallel_hash';
SET parallel_hash_join_threshold = 0;

SELECT 'parallel_right_all_matched';
SELECT count(), countIf(l.id IS NULL)
FROM t_l AS l
RIGHT JOIN t_r_matched AS r ON l.id = r.id;

SELECT 'parallel_full_all_matched';
SELECT count(), countIf(l.id IS NULL), countIf(r.id IS NULL)
FROM t_l AS l
FULL OUTER JOIN t_r_matched AS r ON l.id = r.id;

SELECT 'parallel_right_nullmap';
SELECT count(), countIf(l.id IS NULL)
FROM t_l_null AS l
RIGHT JOIN t_r_null AS r ON l.id = r.id;

SELECT 'parallel_right_unmatched';
SELECT count(), countIf(l.id IS NULL)
FROM t_l AS l
RIGHT JOIN t_r_extra AS r ON l.id = r.id;

SELECT 'parallel_full_unmatched';
SELECT count(), countIf(l.id IS NULL), countIf(r.id IS NULL)
FROM t_l AS l
FULL OUTER JOIN t_r_extra AS r ON l.id = r.id;

DROP TABLE t_l;
DROP TABLE t_r_matched;
DROP TABLE t_r_extra;
DROP TABLE t_l_null;
DROP TABLE t_r_null;
