-- Tags: no-fasttest
-- Tag no-fasttest: needs the DataSketches library.

-- The Apache DataSketches states depend on the order of the input rows (and of the merged sketches):
-- the same set of values fed in a different order produces different sketch bytes and estimates.
-- So `removeRedundantSorting` must keep an `ORDER BY` below these aggregate functions.
-- `uniq` is the control: it is order-independent, so the sorting below it is removed.

SET query_plan_remove_redundant_sorting = 1;
SET optimize_aggregators_of_group_by_keys = 0;

SELECT 'uniq', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT uniq(x) FROM (SELECT number AS x FROM numbers(10) ORDER BY number DESC));
SELECT 'uniqHLL', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT uniqHLL(x) FROM (SELECT number AS x FROM numbers(10) ORDER BY number DESC));
SELECT 'serializedHLL', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT serializedHLL(x) FROM (SELECT number AS x FROM numbers(10) ORDER BY number DESC));
SELECT 'serializedQuantiles', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT serializedQuantiles(x) FROM (SELECT number AS x FROM numbers(10) ORDER BY number DESC));
SELECT 'serializedTDigest', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT serializedTDigest(x) FROM (SELECT number AS x FROM numbers(10) ORDER BY number DESC));
SELECT 'mergeSerializedHLL', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT mergeSerializedHLL(s) FROM (SELECT serializedHLL(number) AS s FROM numbers(10) GROUP BY number % 3 ORDER BY any(number) DESC));
SELECT 'mergeSerializedQuantiles', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT mergeSerializedQuantiles(s) FROM (SELECT serializedQuantiles(number) AS s FROM numbers(10) GROUP BY number % 3 ORDER BY any(number) DESC));
SELECT 'mergeSerializedTDigest', countIf(explain LIKE '%Sorting%') FROM (EXPLAIN SELECT mergeSerializedTDigest(s) FROM (SELECT serializedTDigest(number) AS s FROM numbers(10) GROUP BY number % 3 ORDER BY any(number) DESC));
