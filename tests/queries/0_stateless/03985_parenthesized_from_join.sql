-- Standard SQL allows parentheses around joined table expressions in FROM.
-- Previously this failed with SYNTAX_ERROR.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t2 (x UInt32, c String) ENGINE = MergeTree ORDER BY x;

INSERT INTO t1 VALUES (1, 'one'), (2, 'two');
INSERT INTO t2 VALUES (2, 'dos'), (3, 'tres');

SELECT '--- CROSS JOIN ---';
SELECT a, b, x, c FROM (t1 CROSS JOIN t2) ORDER BY a, x;

SELECT '--- INNER JOIN ---';
SELECT a, b, c FROM (t1 INNER JOIN t2 ON a = x) ORDER BY a;

SELECT '--- With alias ---';
SELECT j.a, j.c FROM (t1 INNER JOIN t2 ON a = x) AS j;

SELECT '--- Nested in outer query ---';
SELECT count() FROM (t1 AS lhs CROSS JOIN t2 AS rhs);

DROP TABLE t1;
DROP TABLE t2;
