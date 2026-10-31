-- Tags: shard
-- grouping() must tell apart GROUPING SETS keys that are different expressions folded to the same value.

SELECT 'constants', grouping(1 + 1, 2 + 0) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((1 + 1), (2 + 0), ()) ORDER BY g;
SELECT 'no aggregate', grouping(1 + 1, 2 + 0) AS g FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((1 + 1), (2 + 0), ()) ORDER BY g;
SELECT 'nested', grouping((1 + 1) + 0, (2 + 0) + 0) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS (((1 + 1) + 0), ((2 + 0) + 0), ()) ORDER BY g;
SELECT 'non-constant', grouping(x + (1 + 1), x + (2 + 0)) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((x + (1 + 1)), (x + (2 + 0)), ()) ORDER BY g;
SELECT 'one set', grouping(1 + 1, 2 + 0) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((1 + 1, 2 + 0), (2 + 0), ()) ORDER BY g;
SELECT 'with column', grouping(1 + 1, 2 + 0, x) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((1 + 1, x), (2 + 0), ()) ORDER BY g;
SELECT 'aliases', grouping(a, b) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((1 + 1 AS a), (2 + 0 AS b), ()) ORDER BY g;
SELECT 'low cardinality', grouping(toLowCardinality('a'), toLowCardinality(concat('a', ''))) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((toLowCardinality('a')), (toLowCardinality(concat('a', ''))), ()) ORDER BY g;
SELECT 'array', grouping([1 + 1], [2 + 0]) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS (([1 + 1]), ([2 + 0]), ()) ORDER BY g;
SELECT 'two tables', grouping(toTypeName(t1.x), toTypeName(t2.x)) AS g, count() FROM (SELECT 1 AS x) AS t1, (SELECT 1 AS x) AS t2 GROUP BY GROUPING SETS ((toTypeName(t1.x)), (toTypeName(t2.x)), ()) ORDER BY g;
SELECT 'same key', grouping(1 + 1) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((1 + 1), (1 + 1), ()) ORDER BY g;
SELECT 'subqueries', grouping((SELECT 1 + 1 AS y), (SELECT 2 + 0 AS y)) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS (((SELECT 1 + 1 AS y)), ((SELECT 2 + 0 AS y)), ()) ORDER BY g;
SELECT 'same subquery', grouping((SELECT 1 + 1)) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS (((SELECT 1 + 1)), ((SELECT 1 + 1)), ()) ORDER BY g;
SELECT 'not standard', grouping(1 + 1, 2 + 0) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((1 + 1), (2 + 0), ()) ORDER BY g SETTINGS force_grouping_standard_compatibility = 0;
SELECT 'use nulls', grouping(1 + 1, 2 + 0) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((1 + 1), (2 + 0), ()) ORDER BY g SETTINGS group_by_use_nulls = 1;
SELECT 'equal to one key', grouping(toUInt16(2)) AS g, count() FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((1 + 1), ()) ORDER BY g;
SELECT grouping(toUInt16(2)) FROM (VALUES (1), (2)) AS t(x) GROUP BY GROUPING SETS ((1 + 1), (2 + 0), ()); -- { serverError BAD_ARGUMENTS }
SELECT 'remote', grouping(1 + 1, 2 + 0) AS g, count() FROM remote('127.0.0.{1,2}', numbers(2)) GROUP BY GROUPING SETS ((1 + 1), (2 + 0), ()) ORDER BY g;
