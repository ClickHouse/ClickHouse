-- Which `IN (subquery)` the in to join rewrite declines, and that each one keeps its set.

SET enable_analyzer = 1;
SET allow_correlated_subqueries = 1;
SET rewrite_in_to_join = 1;

DROP TABLE IF EXISTS p;
DROP TABLE IF EXISTS s;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS w;

CREATE TABLE p (c Tuple(UInt64, UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE s (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t (id UInt64, b UInt64, arr Array(UInt64)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE w (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO p VALUES ((1, 1)), ((2, 9));
INSERT INTO s VALUES (1), (2);
INSERT INTO t VALUES (1, 1, [1, 2]), (2, 3, [5]), (3, 4, [7]);
INSERT INTO w VALUES (1, 1), (2, 3), (3, 2);
SELECT '-- IN over a tuple column and a multi-column subquery';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM p WHERE c IN (SELECT k, v FROM w));

SELECT '-- IN with a constant left argument';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE 1 IN (SELECT k FROM s));

SELECT '-- IN inside PREWHERE';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t PREWHERE id IN (SELECT k FROM s));

SELECT '-- IN with an incomparable subquery column type';
SELECT countIf(explain LIKE '%Join%') > 0, countIf(explain LIKE '%Set%') > 0
FROM (EXPLAIN SELECT count() FROM t WHERE id IN (SELECT toInt8(k) FROM s));

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

DROP TABLE t;
DROP TABLE s;
DROP TABLE w;
DROP TABLE p;
