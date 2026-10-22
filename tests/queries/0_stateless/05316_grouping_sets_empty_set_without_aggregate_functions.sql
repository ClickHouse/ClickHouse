-- Tags: shard

-- An empty grouping set `()` contributes one row even when the query has no aggregate functions.
-- https://github.com/ClickHouse/ClickHouse/issues/61461

SELECT 'keys and empty set', number, grouping(number) FROM numbers(3) GROUP BY GROUPING SETS ((number), ()) ORDER BY ALL;
SELECT 'keys and empty set, use_nulls', number, grouping(number) FROM numbers(3) GROUP BY GROUPING SETS ((number), ()) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT 'issue', a, b, grouping(a, b) FROM (SELECT 1 a, 1 b UNION ALL SELECT 1 a, 2 b) GROUP BY GROUPING SETS ((a, b), (a), ()) ORDER BY ALL;

SELECT 'only empty set', 1 FROM numbers(3) GROUP BY GROUPING SETS (());
SELECT 'only empty set, use_nulls', 1 FROM numbers(3) GROUP BY GROUPING SETS (()) SETTINGS group_by_use_nulls = 1;
SELECT 'aggregate, only empty set', count() FROM numbers(3) GROUP BY GROUPING SETS (());
SELECT 'aggregate, only empty set, flattened', count() FROM (EXPLAIN QUERY TREE SELECT count() FROM numbers(3) GROUP BY GROUPING SETS (())) WHERE explain ILIKE '%grouping_sets%';
SELECT 'three empty sets', 1 FROM (SELECT 1 a UNION ALL SELECT 2 a) GROUP BY GROUPING SETS ((), (), ());
SELECT 'only empty set, totals', 1 FROM numbers(3) GROUP BY GROUPING SETS (()) WITH TOTALS;

-- The unused inner count() is removed by the analyzer.
SELECT 'pruned aggregate', count() FROM (SELECT number, count() FROM numbers(3) GROUP BY GROUPING SETS ((number), ()));
SELECT 'pruned aggregate, two empty sets', count() FROM (SELECT 1 AS x, count() FROM numbers(3) GROUP BY GROUPING SETS ((), ()));
SELECT 'pruned aggregate, one empty set', count() FROM (SELECT 1 AS x, count() FROM numbers(3) GROUP BY GROUPING SETS (())) SETTINGS group_by_use_nulls = 1;

SELECT 'empty input', number FROM numbers(0) GROUP BY GROUPING SETS ((number), ()) SETTINGS group_by_use_nulls = 1;
SELECT 'empty input, only empty set', 1 FROM numbers(0) GROUP BY GROUPING SETS (());
SELECT 'empty input, suppressed', count() FROM (SELECT 1 FROM numbers(0) GROUP BY GROUPING SETS (()) SETTINGS empty_result_for_aggregation_by_empty_set = 1);
SELECT 'empty input, suppressed, keys', count() FROM (SELECT number FROM numbers(0) GROUP BY GROUPING SETS ((number), ()) SETTINGS empty_result_for_aggregation_by_empty_set = 1);

SELECT 'several streams', count(), countIf(isNull(number)) FROM (SELECT number FROM numbers_mt(1000) GROUP BY GROUPING SETS ((number), ())) SETTINGS group_by_use_nulls = 1, max_threads = 4, max_block_size = 100;

SELECT 'distributed', number FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY GROUPING SETS ((number), ()) ORDER BY ALL SETTINGS group_by_use_nulls = 1;
SELECT 'distributed, only empty set', 1 FROM remote('127.0.0.{1,2}', numbers(3)) GROUP BY GROUPING SETS (());

SELECT 'correlated subquery', (SELECT c0) FROM (SELECT 1::Bool) t0(c0) GROUP BY GROUPING SETS ((c0), ()) ORDER BY c0 NULLS LAST SETTINGS group_by_use_nulls = 1;
SELECT 'correlated subquery, no substitution', (SELECT c0) FROM (SELECT 1::Bool) t0(c0) GROUP BY GROUPING SETS ((c0), ()) ORDER BY c0 NULLS LAST SETTINGS group_by_use_nulls = 1, correlated_subqueries_substitute_equivalent_expressions = 0;
