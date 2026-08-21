-- Columns expanded from a matcher (`*`) keep their projection names when the
-- `group_by_use_nulls` rewrite turns them into nullable copies for `ROLLUP`,
-- `CUBE` and `GROUPING SETS`. Without that, the qualification a matcher assigns
-- to columns of joined table expressions is lost: the result columns collide
-- (two columns named `k`) and an outer query can no longer reference them,
-- while the old analyzer resolves such references fine.

SET enable_analyzer = 1;

-- ============================================================
-- Default behavior: qualification added to disambiguate joined columns
-- ============================================================

SET analyzer_compatibility_multiple_joins_qualify_column_names = 0;
SET group_by_use_nulls = 1;

SELECT '=== describe: single join, ROLLUP ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k GROUP BY ROLLUP(ll.k, ll.Date, t1.k));

SELECT '=== outer ref t1.k, single join, ROLLUP ===';
SELECT t1.k FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k GROUP BY ROLLUP(ll.k, ll.Date, t1.k)) ORDER BY t1.k NULLS LAST;

SELECT '=== describe: single join, CUBE ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k GROUP BY CUBE(ll.k, ll.Date, t1.k));

SELECT '=== outer ref t1.k, single join, CUBE ===';
SELECT t1.k FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k GROUP BY CUBE(ll.k, ll.Date, t1.k)) ORDER BY t1.k NULLS LAST;

SELECT '=== describe: single join, GROUPING SETS ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k GROUP BY GROUPING SETS ((ll.k, ll.Date), (t1.k)));

SELECT '=== outer ref t1.k, single join, GROUPING SETS ===';
SELECT t1.k FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k GROUP BY GROUPING SETS ((ll.k, ll.Date), (t1.k))) ORDER BY t1.k NULLS LAST;

-- Control: a plain `GROUP BY` never enters the nullable rewrite.
SELECT '=== describe: single join, plain GROUP BY ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k GROUP BY ll.k, ll.Date, t1.k);

-- Control: the same `ROLLUP` query without `group_by_use_nulls`.
SET group_by_use_nulls = 0;

SELECT '=== describe: single join, ROLLUP, group_by_use_nulls = 0 ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k GROUP BY ROLLUP(ll.k, ll.Date, t1.k));

-- ============================================================
-- The same, with the qualification forced by
-- `analyzer_compatibility_multiple_joins_qualify_column_names`
-- ============================================================

SET analyzer_compatibility_multiple_joins_qualify_column_names = 1;
SET group_by_use_nulls = 1;

SELECT '=== describe: two joins, ROLLUP, setting ON ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k GROUP BY ROLLUP(ll.k, ll.Date, t1.k, t2.k));

SELECT '=== outer ref ll.Date, two joins, ROLLUP, setting ON ===';
SELECT ll.Date FROM (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k GROUP BY ROLLUP(ll.k, ll.Date, t1.k, t2.k)) ORDER BY ll.Date NULLS LAST;

SELECT '=== describe: two joins, CUBE, setting ON ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k GROUP BY CUBE(ll.k, ll.Date, t1.k, t2.k));

SELECT '=== describe: two joins, GROUPING SETS, setting ON ===';
DESCRIBE (SELECT * FROM (SELECT 1 AS k, 'D' AS Date) AS ll LEFT JOIN (SELECT 1 AS k) AS t1 ON ll.k = t1.k LEFT JOIN (SELECT 1 AS k) AS t2 ON ll.k = t2.k GROUP BY GROUPING SETS ((ll.k, ll.Date), (t1.k, t2.k)));
