-- An if chain or an ORDER BY tuple that contains a GROUP BY key reads the key from the aggregation result.

SET optimize_if_transform_strings_to_enum = 0;

-- A constant-condition `if` key nested in an `if` chain (#125803).
SELECT n, if(n = 5, toString(n), if(1, if(n > 1, 'p', 'q'), 'z')) AS e, count() FROM (SELECT arrayJoin([1, 2]) AS n) GROUP BY ROLLUP(n, if(1, if(n > 1, 'p', 'q'), 'z')) ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 1;
SELECT count() FROM (SELECT n, if(n = 5, toString(n), if(1, if(n > 1, 'p', 'q'), 'z')) AS e FROM (SELECT arrayJoin([1, 2]) AS n) GROUP BY ROLLUP(n, if(1, if(n > 1, 'p', 'q'), 'z')) HAVING e IS NULL SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 1);

-- A plain `if` key nested in an `if` chain.
SELECT n, if(n = 5, toString(n), if(n > 1, 'p', 'q')) AS e, count() FROM (SELECT arrayJoin([1, 2]) AS n) GROUP BY ROLLUP(n, if(n > 1, 'p', 'q')) ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 1;
SELECT n, if(n = 5, [toUInt8(n)], if(n > 1, [2], [3])) AS e, count() FROM (SELECT arrayJoin([1, 2]) AS n) GROUP BY ROLLUP(n, if(n > 1, [2], [3])) ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 1;
SELECT n, if(n = 5, toString(n), if(m > 1, 'p', 'q')) AS e, count() FROM (SELECT number AS n, number % 3 AS m FROM numbers(4)) GROUP BY ROLLUP(n, if(m > 1, 'p', 'q')) ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 1;
SELECT n, if(n = 5, toString(n), if(m > 1, 'p', 'q')) AS e FROM (SELECT number AS n, number % 3 AS m FROM numbers(4)) GROUP BY n, if(m > 1, 'p', 'q') ORDER BY n SETTINGS group_by_use_nulls = 0, optimize_if_chain_to_multiif = 1;

-- A key whose `then` branch is itself a chain.
SELECT n FROM (SELECT number AS n, number % 2 = 1 AS a, number % 3 = 1 AS p, number % 5 = 1 AS q FROM numbers(6)) GROUP BY n, if(a, if(p, 'u', if(q, 'v', 'w')), 'y') ORDER BY if(n = 5, 'x', if(a, if(p, 'u', if(q, 'v', 'w')), 'y')), n SETTINGS group_by_use_nulls = 0, optimize_if_chain_to_multiif = 1;
SELECT n, if(a, if(p, 'u', if(q, 'v', 'w')), 'y') AS k FROM (SELECT number AS n, number % 2 = 1 AS a, number % 3 = 1 AS p, number % 5 = 1 AS q FROM numbers(6)) GROUP BY n, k ORDER BY if(n = 5, 'x', k), n SETTINGS group_by_use_nulls = 0, optimize_if_chain_to_multiif = 1, enable_identifier_resolve_cache = 1;

-- A chain without a key is still rewritten.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT n, if(n = 1, 'a', if(n = 2, 'b', 'c')) FROM (SELECT arrayJoin([1, 2]) AS n) GROUP BY ROLLUP(n) SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 1) WHERE explain ILIKE '%function_name: multiIf%';

-- A tuple key in ORDER BY (#125804).
SELECT if(1, tuple(a), tuple(0)) AS k, count() FROM (SELECT number % 3 AS a FROM numbers(10)) GROUP BY ROLLUP(k) ORDER BY k SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 0;
SELECT a, if(1, tuple(a), tuple(0)) AS k FROM (SELECT number % 2 AS a FROM numbers(4)) GROUP BY ROLLUP(a, k) ORDER BY k, a SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 0;
SELECT if(1, tuple(a), tuple(0)) AS k, count() FROM (SELECT number % 3 AS a FROM numbers(10)) GROUP BY k ORDER BY k SETTINGS group_by_use_nulls = 0, optimize_if_chain_to_multiif = 0;
SELECT if(1, tuple(a), tuple(0)) AS k, count() FROM (SELECT number % 3 AS a FROM numbers(10)) GROUP BY ROLLUP(k) ORDER BY k SETTINGS group_by_use_nulls = 0, optimize_if_chain_to_multiif = 0;
SELECT if(1, tuple(a), tuple(0)) AS k, count() FROM (SELECT number % 3 AS a FROM numbers(10)) GROUP BY CUBE(k) ORDER BY ALL SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 0;
SELECT if(1, tuple(a, b), tuple(0, 0)) AS k, count() FROM (SELECT number % 2 AS a, number % 3 AS b FROM numbers(12)) GROUP BY GROUPING SETS ((k), ()) ORDER BY k SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 0;

-- A tuple of keys that is not itself a key is still split.
SELECT count() FROM (EXPLAIN QUERY TREE SELECT a, b FROM (SELECT number % 2 AS a, number % 3 AS b FROM numbers(12)) GROUP BY ROLLUP(a, b) ORDER BY tuple(a, b) SETTINGS group_by_use_nulls = 1, optimize_if_chain_to_multiif = 0) WHERE explain ILIKE '%function_name: tuple%';
