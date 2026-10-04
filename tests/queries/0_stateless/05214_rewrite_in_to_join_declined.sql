-- Which `IN (subquery)` the in to join rewrite declines, and that each one keeps its set.

SET enable_analyzer = 1;
SET allow_correlated_subqueries = 1;
SET rewrite_in_to_join = 1;

DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS w;

CREATE TABLE s (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t (id UInt64, b UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE w (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO s VALUES (1), (2);
INSERT INTO t VALUES (1, 1, [1, 2]), (2, 3, [5]), (3, 4, [7]);
INSERT INTO w VALUES (1, 1), (2, 3), (3, 2);
SELECT '-- IN with a constant left argument';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE 1 IN (SELECT k FROM s));

SELECT '-- IN inside PREWHERE';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t PREWHERE id IN (SELECT k FROM s));

SELECT '-- IN inside a lambda';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE arrayExists(z -> z IN (SELECT k FROM s), arr));

SELECT '-- `nullIn`';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT k FROM s) SETTINGS transform_null_in = 1);

SELECT '-- IN inside INTERPOLATE';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT id, b FROM t ORDER BY id WITH FILL FROM 1 TO 5 INTERPOLATE (b AS b + (b IN (SELECT k FROM s))));

SELECT groupArray((id, b)) FROM (SELECT id, b FROM t ORDER BY id WITH FILL FROM 1 TO 5 INTERPOLATE (b AS b + (b IN (SELECT k FROM s))));

SELECT '-- A `DateTime64` key against a column that cannot hold its sub-second part';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE toDateTime64(id, 3) IN (SELECT toDateTime(k) FROM s));

SELECT '-- `Array(UInt8)` against `Array(Bool)`, a cast that throws on a value the element type cannot hold';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT CAST([arr[1]], 'Array(UInt8)') IN (SELECT [true]) AS c FROM t);

DROP TABLE t;
DROP TABLE s;
DROP TABLE w;
