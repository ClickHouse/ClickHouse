SET enable_analyzer = 1;

-- https://github.com/ClickHouse/ClickHouse/issues/82499
-- Window functions with `group_by_use_nulls = 1` and CUBE/ROLLUP/GROUPING SETS
-- could crash because the aggregate function was created with non-nullable argument
-- types, but the actual data columns became nullable after GROUP BY.

-- Original reproducer from the issue:
CREATE DICTIONARY d0 (c0 Int) PRIMARY KEY (c0) SOURCE(NULL()) LAYOUT(HASHED()) LIFETIME(1);
SELECT min('a') OVER () FROM d0 GROUP BY 'a', c0 WITH CUBE WITH TOTALS SETTINGS group_by_use_nulls = 1;
DROP DICTIONARY d0;

SELECT '---';

SELECT min('a') OVER () FROM numbers(3) GROUP BY 'a', number WITH CUBE WITH TOTALS SETTINGS group_by_use_nulls = 1;

SELECT '---';

WITH 'a' AS x SELECT leadInFrame(x) OVER (ORDER BY x NULLS FIRST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) GROUP BY ROLLUP(x) ORDER BY 1 NULLS LAST SETTINGS group_by_use_nulls = 1;

SELECT '---';

SELECT max('hello') OVER (), min('world') OVER () FROM numbers(2) GROUP BY number WITH ROLLUP SETTINGS group_by_use_nulls = 1;

SELECT '---';

SELECT any('test') OVER () FROM numbers(2) GROUP BY GROUPING SETS(('test', number), ('test')) SETTINGS group_by_use_nulls = 1;

SELECT '---';

WITH 'x' AS v SELECT lag(v) OVER (ORDER BY v), lead(v) OVER (ORDER BY v) GROUP BY ROLLUP(v) ORDER BY 1 NULLS FIRST SETTINGS group_by_use_nulls = 1, enable_analyzer = 1; -- lag/lead require the analyzer

SELECT '---';

SELECT min(1) OVER (), max(42) OVER (), sum(1) OVER () FROM numbers(2) GROUP BY number WITH ROLLUP SETTINGS group_by_use_nulls = 1;
