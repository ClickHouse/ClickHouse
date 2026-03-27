SET join_use_nulls = 1;
SET enable_analyzer = 1;
SET join_algorithm = 'parallel_hash';
SET query_plan_join_swap_table = 0;

-- 1) Small dataset: RIGHT OUTER ALL
DROP TABLE IF EXISTS t_l_small;
DROP TABLE IF EXISTS t_r_small;
CREATE TABLE t_l_small (id UInt32, value String) ENGINE = Memory;
CREATE TABLE t_r_small (id UInt32, description String) ENGINE = Memory;
INSERT INTO t_l_small VALUES (1, 'A'), (2, 'B'), (3, 'C');
INSERT INTO t_r_small VALUES (2, 'Second'), (3, 'Third'), (4, 'Fourth'), (5, 'Fifth');

SELECT 'right_all_small';
SELECT l.id, l.value, r.description
FROM t_l_small AS l
RIGHT JOIN t_r_small AS r ON l.id = r.id
ORDER BY r.id, l.id;

-- Explain plan for a small RIGHT OUTER join (should use ConcurrentHashJoin / parallel_hash)
SELECT 'explain_right_all_small';
EXPLAIN actions=1, keep_logical_steps=0
SELECT l.id, l.value, r.description
FROM t_l_small AS l
RIGHT JOIN t_r_small AS r ON l.id = r.id
ORDER BY r.id, l.id;

-- 2) Small dataset: FULL OUTER ALL
SELECT 'full_all_small';
SELECT l.id, l.value, r.description
FROM t_l_small AS l
FULL OUTER JOIN t_r_small AS r ON l.id = r.id
ORDER BY coalesce(l.id, r.id), r.id;

-- 3) RIGHT ANY with duplicates on left (identical values to avoid nondeterminism), aggregated checks
DROP TABLE IF EXISTS t_l_any;
DROP TABLE IF EXISTS t_r_any;
CREATE TABLE t_l_any (id UInt32, value String) ENGINE = Memory;
CREATE TABLE t_r_any (id UInt32, description String) ENGINE = Memory;
INSERT INTO t_l_any VALUES (2, 'B'), (2, 'B'), (3, 'C'), (10, 'X');
INSERT INTO t_r_any VALUES (2, 'Second'), (3, 'Third'), (4, 'Fourth');

SELECT 'right_any_small';
SELECT
    count(),
    countIf(isNull(l.value))
FROM t_l_any AS l
RIGHT ANY JOIN t_r_any AS r ON l.id = r.id;

-- 4) RIGHT OUTER with additional ON filter
DROP TABLE IF EXISTS t_l_filter;
DROP TABLE IF EXISTS t_r_filter;
CREATE TABLE t_l_filter (id UInt32, value String) ENGINE = Memory;
CREATE TABLE t_r_filter (id UInt32, description String) ENGINE = Memory;
INSERT INTO t_l_filter VALUES (2, 'B'), (3, 'C'), (4, 'D');
INSERT INTO t_r_filter VALUES (2, 'Second'), (3, 'Third'), (4, 'Fourth');

SELECT 'right_all_with_on_filter';
SELECT l.id, l.value, r.description
FROM t_l_filter AS l
RIGHT JOIN t_r_filter AS r ON l.id = r.id AND r.description LIKE 'F%'
ORDER BY r.id;

-- 5) RIGHT OUTER with null keys on right
DROP TABLE IF EXISTS t_l_null;
DROP TABLE IF EXISTS t_r_null;
CREATE TABLE t_l_null (id UInt32, v String) ENGINE = Memory;
CREATE TABLE t_r_null (id Nullable(UInt32), d String) ENGINE = Memory;
INSERT INTO t_l_null VALUES (1, 'A'), (2, 'B');
INSERT INTO t_r_null VALUES (1, 'one'), (NULL, 'null1'), (3, 'three');

SELECT 'right_all_null_keys';
SELECT l.id, l.v, r.d
FROM t_l_null AS l
RIGHT JOIN t_r_null AS r ON l.id = r.id
ORDER BY r.d;

-- 6) Composite key RIGHT OUTER ALL
DROP TABLE IF EXISTS t_l_cmp;
DROP TABLE IF EXISTS t_r_cmp;
CREATE TABLE t_l_cmp (id UInt32, grp UInt8, val String) ENGINE = Memory;
CREATE TABLE t_r_cmp (id UInt32, grp UInt8, descr String) ENGINE = Memory;
INSERT INTO t_l_cmp VALUES (1, 1, 'a'), (1, 2, 'b'), (2, 1, 'c');
INSERT INTO t_r_cmp VALUES (1, 1, 'r11'), (2, 1, 'r21'), (3, 1, 'r31');

SELECT 'right_all_composite_keys';
SELECT l.id, l.grp, l.val, r.descr
FROM t_l_cmp AS l
RIGHT JOIN t_r_cmp AS r ON (l.id = r.id) AND (l.grp = r.grp)
ORDER BY r.id, r.grp;

-- 7) Large volume RIGHT OUTER ALL (aggregated)
SELECT 'right_all_large';
SELECT
    count(),
    countIf(isNull(l.id)),
    sum(coalesce(l.id, 0)),
    sum(r.id)
FROM
    (SELECT number AS id FROM numbers(15000)) AS l
RIGHT JOIN
    (SELECT number AS id FROM numbers(20000)) AS r
ON l.id = r.id;

-- 8) Large volume FULL OUTER ALL (aggregated)
SELECT 'full_all_large';
SELECT
    count(),
    countIf(isNull(l.id)),   -- right-only
    countIf(isNull(r.id))    -- left-only
FROM
    (SELECT number AS id FROM numbers(15000)) AS l
FULL OUTER JOIN
    (SELECT number AS id FROM numbers(15500)) AS r
ON l.id = r.id;

-- Cleanup
DROP TABLE IF EXISTS t_l_small;
DROP TABLE IF EXISTS t_r_small;
DROP TABLE IF EXISTS t_l_any;
DROP TABLE IF EXISTS t_r_any;
DROP TABLE IF EXISTS t_l_filter;
DROP TABLE IF EXISTS t_r_filter;
DROP TABLE IF EXISTS t_l_null;
DROP TABLE IF EXISTS t_r_null;
DROP TABLE IF EXISTS t_l_cmp;
DROP TABLE IF EXISTS t_r_cmp;

SELECT 'Consistency of results with HashJoin for multiple conditions';

SET allow_experimental_analyzer = 1;
SET join_use_nulls = 1;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
CREATE TABLE l (k UInt8, v UInt8) ENGINE = Memory;
CREATE TABLE r (k UInt8, v UInt8) ENGINE = Memory;

INSERT INTO l SELECT toUInt8(number), toUInt8(number) FROM numbers(200);
INSERT INTO r SELECT toUInt8(number), toUInt8(number) FROM numbers(200);

SET max_threads = 8; SET join_algorithm = 'hash';
SELECT 'hash' AS alg, count() AS cnt
FROM l RIGHT JOIN r ON l.k = r.k AND l.v > r.v;

SET join_algorithm = 'parallel_hash';
SELECT 'parallel_hash' AS alg, count() AS cnt
FROM l RIGHT JOIN r ON l.k = r.k AND l.v > r.v;

SET join_algorithm = 'hash';
SELECT 'hash right-only', countIf(l.k IS NULL) FROM l RIGHT JOIN r ON l.k = r.k AND l.v > r.v;
SET join_algorithm = 'parallel_hash';
SELECT 'parallel right-only', countIf(l.k IS NULL) FROM l RIGHT JOIN r ON l.k = r.k AND l.v > r.v;

SET join_algorithm = 'hash';
SELECT 'hash', count() FROM l FULL OUTER JOIN r ON l.k = r.k AND l.v > r.v;
SET join_algorithm = 'parallel_hash';
SELECT 'parallel_hash', count() FROM l FULL OUTER JOIN r ON l.k = r.k AND l.v > r.v;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
