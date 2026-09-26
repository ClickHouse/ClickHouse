-- Tags: no-old-analyzer
-- no-old-analyzer: the deferred `group_by_use_nulls` REPLACE rewrite exists only in the analyzer.

-- With `group_by_use_nulls = 0` every projection matcher rewrites the sibling clauses as it is
-- resolved, so a later matcher still rewrites the identifiers that an earlier matcher's
-- replacement expression introduced. The deferred `group_by_use_nulls = 1` path must replay the
-- rewrites in projection order for the same reason: flattening all matchers into a single
-- substitution pass leaves `HAVING max(a) + 1 > 0` instead of `HAVING max(0) + 1 > 0`, which
-- silently filters out every row.

DROP TABLE IF EXISTS t_05037;
CREATE TABLE t_05037 (k Int32, a Int32, b Int32) ENGINE = Memory;
INSERT INTO t_05037 VALUES (1, -5, 7), (2, -3, 9);

-- The first matcher maps `b -> max(a) + 1`, the second one maps `a -> 0`, so `HAVING b > 0`
-- must end up as `HAVING max(0) + 1 > 0`, which keeps every group despite the negative values.
SELECT k, COLUMNS('^b$') REPLACE (max(a) + 1 AS b), COLUMNS('^a$') REPLACE (0 AS a)
FROM t_05037
GROUP BY k WITH ROLLUP
ORDER BY k
SETTINGS group_by_use_nulls = 0;

SELECT k, COLUMNS('^b$') REPLACE (max(a) + 1 AS b), COLUMNS('^a$') REPLACE (0 AS a)
FROM t_05037
GROUP BY k WITH ROLLUP
HAVING b > 0
ORDER BY k
SETTINGS group_by_use_nulls = 0;

SELECT k, COLUMNS('^b$') REPLACE (max(a) + 1 AS b), COLUMNS('^a$') REPLACE (0 AS a)
FROM t_05037
GROUP BY k WITH ROLLUP
HAVING b > 0
ORDER BY k
SETTINGS group_by_use_nulls = 1;

DROP TABLE t_05037;
