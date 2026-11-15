-- Tags: no-fasttest, no-cpu-s390x
-- ^ DataSketches is not built in fast-test builds.

-- The HIP estimate and the bytes of a `uniqApacheHLL` sketch depend on the order of the updates and of the merges, so the planner must keep an `ORDER BY` below it,
-- also below its `-State`, `-Merge` and `-If` forms. `uniq` is the control: its result does not depend on the order, so the sorting below it is removed.

SET query_plan_remove_redundant_sorting = 1;
SET optimize_aggregators_of_group_by_keys = 0;
SELECT 'uniq', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT uniq(x) FROM (SELECT number AS x FROM numbers(10) ORDER BY number DESC));
SELECT 'uniqApacheHLL', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT uniqApacheHLL(x) FROM (SELECT number AS x FROM numbers(10) ORDER BY number DESC));
SELECT 'uniqApacheHLL params', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT uniqApacheHLL(14, 'HLL_8')(x) FROM (SELECT number AS x FROM numbers(10) ORDER BY number DESC));
SELECT 'uniqApacheHLLState', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT uniqApacheHLLState(x) FROM (SELECT number AS x FROM numbers(10) ORDER BY number DESC));
SELECT 'uniqApacheHLLMerge', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT uniqApacheHLLMerge(s) FROM (SELECT uniqApacheHLLState(number) AS s FROM numbers(10) GROUP BY number % 3 ORDER BY number % 3 DESC));
SELECT 'uniqApacheHLLIf', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT uniqApacheHLLIf(x, x > 2) FROM (SELECT number AS x FROM numbers(10) ORDER BY number DESC));
