-- Tags: no-old-analyzer
-- Tag no-old-analyzer: the fix is in the planner; `enable_analyzer = 0` still expands CUBE and
-- ROLLUP from the deduplicated key list.
-- CUBE and ROLLUP are defined over the GROUP BY list as written: CUBE takes its power set and
-- ROLLUP its prefixes. Repeated expressions were deduplicated before either was expanded, so the
-- number of grouping sets was computed from the distinct keys and the extra sets were lost. The
-- same duplication written out as GROUPING SETS was kept, so the two spellings disagreed.
-- https://github.com/ClickHouse/ClickHouse/issues/117904

SELECT 'CUBE over a repeated key';
-- Power set of [number, number] is 4 sets; the two one-element subsets both keep `number`, so the
-- rows are 2 + 2 + 2 + 1.
SELECT number, count() AS c FROM numbers(2) GROUP BY CUBE(number, number) ORDER BY number, c;

SELECT 'ROLLUP over a repeated key';
-- Prefixes of [number, number] are 3 sets; dropping the second occurrence still leaves the first,
-- so `number` survives it and the rows are 2 + 2 + 1.
SELECT number, count() AS c FROM numbers(2) GROUP BY ROLLUP(number, number) ORDER BY number, c;

SELECT 'the same sets written out explicitly agree';
SELECT number, count() AS c FROM numbers(2) GROUP BY GROUPING SETS ((number), (number)) ORDER BY number;

SELECT 'GROUPING reports the key as present while any occurrence of it is';
SELECT number, count() AS c, GROUPING(number) AS g
FROM numbers(2) GROUP BY CUBE(number, number) ORDER BY g, number, c;

SELECT 'a key repeated among others';
-- CUBE(a, b, a): 8 subsets over the written list, and `a` is kept by any subset holding either of
-- its positions.
SELECT a, b, count() AS c FROM (SELECT number % 2 AS a, number % 2 AS b FROM numbers(4))
GROUP BY CUBE(a, b, a) ORDER BY a, b, c;

SELECT 'ROLLUP with the repeat in the middle';
SELECT a, b, count() AS c FROM (SELECT number % 2 AS a, number % 2 AS b FROM numbers(4))
GROUP BY ROLLUP(a, b, a) ORDER BY a, b, c;

SELECT 'no repetition is unchanged';
SELECT number, count() AS c FROM numbers(2) GROUP BY CUBE(number) ORDER BY number, c;
SELECT number, count() AS c FROM numbers(2) GROUP BY ROLLUP(number) ORDER BY number, c;

SELECT 'group_by_use_nulls keeps the extra sets';
SELECT number, count() AS c FROM numbers(2) GROUP BY CUBE(number, number)
ORDER BY number NULLS LAST, c SETTINGS group_by_use_nulls = 1;

SELECT 'WITH TOTALS still works over a repeated key';
SELECT number, count() AS c FROM numbers(2) GROUP BY CUBE(number, number) WITH TOTALS ORDER BY number, c;
