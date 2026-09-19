-- https://github.com/ClickHouse/ClickHouse/issues/117904
-- `CUBE` takes the power set and `ROLLUP` the prefixes of the `GROUP BY` list as written, repeats included.

SELECT number, count() AS c FROM numbers(2) GROUP BY CUBE(number, number) ORDER BY number, c;
SELECT number, count() AS c FROM numbers(2) GROUP BY ROLLUP(number, number) ORDER BY number, c;

-- The same sets spelled out as `GROUPING SETS`.
SELECT number, count() AS c FROM numbers(2) GROUP BY GROUPING SETS ((number), (number)) ORDER BY number;

SELECT number, count() AS c, GROUPING(number) AS g FROM numbers(2) GROUP BY CUBE(number, number) ORDER BY g, number, c;

-- The repeat is in the middle, so the order of the positions matters, not only their count.
SELECT a, b, count() AS c FROM (SELECT number % 2 AS a, number % 2 AS b FROM numbers(4)) GROUP BY ROLLUP(a, b, a) ORDER BY a, b, c;

-- With `group_by_use_nulls` the dropped key reads `NULL` and the extra sets are kept.
SELECT number, count() AS c FROM numbers(2) GROUP BY CUBE(number, number) ORDER BY number NULLS LAST, c SETTINGS group_by_use_nulls = 1;

SELECT number, count() AS c FROM numbers(2) GROUP BY CUBE(number, number) WITH TOTALS ORDER BY number, c;
