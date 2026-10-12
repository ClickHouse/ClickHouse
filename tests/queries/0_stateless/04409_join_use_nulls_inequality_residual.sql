-- Tests that `join_use_nulls = 1` works for JOINs whose ON section mixes an
-- equality key with an inequality (residual) condition referencing both tables,
-- e.g. `t1.a = t2.a AND t1.c >= t2.c`. This used to throw
-- "JOIN ON expression contains column from left and right table, which is not
-- supported with `join_use_nulls`".
-- https://github.com/ClickHouse/ClickHouse/issues/74730

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t1 VALUES (1, 1, 1), (2, 2, 2), (3, 3, 5);

CREATE TABLE t2 (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t2 VALUES (2, 2, 2), (3, 3, 3), (3, 3, 9);

SET enable_analyzer = 1;
SET join_use_nulls = 1;

-- { echoOn }

-- The exact query from the issue. The right-side columns must become Nullable.
SELECT t1.a, t1.c, t2.a, t2.c, toTypeName(t2.a), toTypeName(t2.c)
FROM t1 LEFT JOIN t2 ON (t1.a = t2.a AND t1.c >= t2.c)
ORDER BY ALL;

-- RIGHT JOIN: the left-side columns must become Nullable.
SELECT t1.a, t2.a, toTypeName(t1.a)
FROM t1 RIGHT JOIN t2 ON (t1.a = t2.a AND t1.c >= t2.c)
ORDER BY ALL;

-- FULL JOIN: both sides must become Nullable.
SELECT t1.a, t2.a, toTypeName(t1.a), toTypeName(t2.a)
FROM t1 FULL JOIN t2 ON (t1.a = t2.a AND t1.c >= t2.c)
ORDER BY ALL;

-- INNER JOIN with a residual inequality.
SELECT t1.a, t2.a
FROM t1 INNER JOIN t2 ON (t1.a = t2.a AND t1.c >= t2.c)
ORDER BY ALL;

-- Other inequality operators.
SELECT t1.a, t2.a, t2.c FROM t1 LEFT JOIN t2 ON (t1.a = t2.a AND t1.c < t2.c) ORDER BY ALL;
SELECT t1.a, t2.a, t2.c FROM t1 LEFT JOIN t2 ON (t1.a = t2.a AND t1.c != t2.c) ORDER BY ALL;

-- Expression on both sides of the inequality.
SELECT t1.a, t2.a, t2.c FROM t1 LEFT JOIN t2 ON (t1.a = t2.a AND t1.c + 1 >= t2.c) ORDER BY ALL;

-- More than one residual inequality referencing both tables.
SELECT t1.a, t2.a FROM t1 FULL JOIN t2 ON (t1.a = t2.a AND t1.c >= t2.c AND t1.b <= t2.b) ORDER BY ALL;

-- Disjunction mixing both tables inside the residual condition.
SELECT t1.a, t2.a, t2.c FROM t1 LEFT JOIN t2 ON (t1.a = t2.a AND (t1.c >= t2.c OR t1.b = t2.b)) ORDER BY ALL;

-- The same with the grace_hash algorithm.
SELECT t1.a, t2.a FROM t1 LEFT JOIN t2 ON (t1.a = t2.a AND t1.c >= t2.c) ORDER BY ALL SETTINGS join_algorithm = 'grace_hash';

-- { echoOff }

DROP TABLE t1;
DROP TABLE t2;
