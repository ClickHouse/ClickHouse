-- Tags: no-old-analyzer
-- no-old-analyzer: the deferred `group_by_use_nulls` REPLACE rewrite exists only in the analyzer.

-- A projection item that follows a matcher must see the identifiers the matcher's `REPLACE`
-- introduced, and it must see them identically whether the `REPLACE` rewrite happens eagerly
-- (`group_by_use_nulls = 0`) or through the deferred pass (`group_by_use_nulls = 1`). Both
-- settings must therefore agree on every query below.

SET enable_positional_arguments = 0;

DROP TABLE IF EXISTS t_05055;
CREATE TABLE t_05055 (k Int32, c Int32, d Int32) ENGINE = Memory;
INSERT INTO t_05055 VALUES (1, 1, 2), (2, 3, 4);

SELECT '--- later projection item over an earlier REPLACE, group_by_use_nulls = 0 ---';
SELECT * REPLACE (10 AS c), c + 1
FROM (SELECT 1 AS c)
GROUP BY c WITH ROLLUP
SETTINGS group_by_use_nulls = 0;

SELECT '--- the same with group_by_use_nulls = 1 ---';
SELECT * REPLACE (10 AS c), c + 1
FROM (SELECT 1 AS c)
GROUP BY c WITH ROLLUP
SETTINGS group_by_use_nulls = 1;

SELECT '--- a later matcher over an earlier REPLACE, group_by_use_nulls = 0 ---';
SELECT k, COLUMNS('^c$') REPLACE (10 AS c), COLUMNS('^d$') REPLACE (max(c) + 1 AS d)
FROM t_05055
GROUP BY k WITH ROLLUP
ORDER BY k
SETTINGS group_by_use_nulls = 0;

SELECT '--- the same with group_by_use_nulls = 1 ---';
SELECT k, COLUMNS('^c$') REPLACE (10 AS c), COLUMNS('^d$') REPLACE (max(c) + 1 AS d)
FROM t_05055
GROUP BY k WITH ROLLUP
ORDER BY k
SETTINGS group_by_use_nulls = 1;

SELECT '--- a later projection item over an earlier REPLACE with an aggregate, group_by_use_nulls = 0 ---';
SELECT k, COLUMNS('^c$') REPLACE (max(d) AS c), c + 1
FROM t_05055
GROUP BY k WITH ROLLUP
ORDER BY k
SETTINGS group_by_use_nulls = 0;

SELECT '--- the same with group_by_use_nulls = 1 ---';
SELECT k, COLUMNS('^c$') REPLACE (max(d) AS c), c + 1
FROM t_05055
GROUP BY k WITH ROLLUP
ORDER BY k
SETTINGS group_by_use_nulls = 1;

DROP TABLE t_05055;
