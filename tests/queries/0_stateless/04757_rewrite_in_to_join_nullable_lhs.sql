-- { echo }
SET enable_analyzer = 1;
SET allow_experimental_correlated_subqueries = 1;
SET rewrite_in_to_join = 1;

DROP TABLE IF EXISTS t_04757;
CREATE TABLE t_04757 (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04757 VALUES (0), (1), (NULL), (5);

-- NULL IN (non-empty) is NULL, so the NULL row must be NULL and must not pass a NOT IN filter.
SELECT x, x IN (SELECT number FROM numbers(3)) AS r FROM t_04757 ORDER BY x;
SELECT x, x NOT IN (SELECT number FROM numbers(3)) AS r FROM t_04757 ORDER BY x;
SELECT x FROM t_04757 WHERE x NOT IN (SELECT number FROM numbers(3)) ORDER BY x;
SELECT DISTINCT toTypeName(x IN (SELECT number FROM numbers(3))) FROM t_04757;

DROP TABLE t_04757;
