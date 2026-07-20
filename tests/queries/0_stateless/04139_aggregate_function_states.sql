-- Exercise ColumnAggregateFunction operations:
-- -State / -Merge round-trip, partial state merging across partitions,
-- storage in AggregatingMergeTree, and state serialization in Native format.

SELECT '--- uniq -State -> -Merge across partitions ---';
SELECT uniqMerge(s) FROM (
    SELECT uniqState(number) AS s FROM numbers(100)
    UNION ALL
    SELECT uniqState(number * 2) FROM numbers(100));

SELECT '--- quantile(0.5) -State -> -Merge ---';
SELECT quantileMerge(0.5)(s) FROM (
    SELECT quantileState(0.5)(number) AS s FROM numbers(100));

SELECT '--- avg / sum / count -State -> -Merge ---';
SELECT avgMerge(a), sumMerge(s), countMerge(c) FROM (
    SELECT avgState(number) AS a, sumState(number) AS s, countState(number) AS c
    FROM numbers(10));

SELECT '--- groupArray -State -> -Merge (array state column) ---';
SELECT arraySort(groupArrayMerge(s)) FROM (
    SELECT groupArrayState(number) AS s FROM numbers(5)
    UNION ALL
    SELECT groupArrayState(number + 100) FROM numbers(3));

SELECT '--- AggregatingMergeTree: insert states, merge at read ---';
DROP TABLE IF EXISTS agg_states;
CREATE TABLE agg_states
(
    g String,
    s AggregateFunction(uniq, UInt64),
    q AggregateFunction(quantile(0.5), UInt64),
    a AggregateFunction(avg, UInt64)
) ENGINE = AggregatingMergeTree ORDER BY g;

INSERT INTO agg_states
SELECT 'g1' AS g, uniqState(number) AS s, quantileState(0.5)(number) AS q, avgState(number) AS a
FROM numbers(50);
INSERT INTO agg_states
SELECT 'g2', uniqState(number * 2), quantileState(0.5)(number * 2), avgState(number * 2)
FROM numbers(50);
INSERT INTO agg_states
SELECT 'g1', uniqState(number + 100), quantileState(0.5)(number + 100), avgState(number + 100)
FROM numbers(50);

SELECT '--- before OPTIMIZE: merged at query time ---';
SELECT g, uniqMerge(s), quantileMerge(0.5)(q), avgMerge(a)
FROM agg_states GROUP BY g ORDER BY g;

OPTIMIZE TABLE agg_states FINAL;

SELECT '--- after OPTIMIZE FINAL: merged by engine ---';
SELECT g, uniqMerge(s), quantileMerge(0.5)(q), avgMerge(a)
FROM agg_states GROUP BY g ORDER BY g;

SELECT '--- SELECT FINAL returns merged-final state ---';
SELECT g, uniqMerge(s) FROM agg_states FINAL GROUP BY g ORDER BY g;

SELECT '--- sumMapState / sumMapMerge: Map-typed aggregate ---';
SELECT sumMapMerge(m) FROM (
    SELECT sumMapState(['a', 'b'], [1, 2]::Array(UInt32)) AS m
    UNION ALL
    SELECT sumMapState(['b', 'c'], [10, 20]::Array(UInt32)));

SELECT '--- argMinState / argMaxState -> -Merge ---';
SELECT argMinMerge(amin), argMaxMerge(amax) FROM (
    SELECT argMinState(val, key) AS amin, argMaxState(val, key) AS amax
    FROM (SELECT 'a' AS val, 3 AS key UNION ALL SELECT 'b', 1 UNION ALL SELECT 'c', 5));

SELECT '--- topK -State -> -Merge ---';
-- topK over unique numbers is non-deterministic (all rows tie at count=1).
-- Use a weighted distribution so the top-3 are stable.
SELECT arraySort(topKMerge(3)(s)) FROM (
    SELECT topKState(3)(number) AS s FROM (
        SELECT arrayJoin([1, 1, 1, 1, 2, 2, 2, 3, 3, 4]) AS number));

SELECT '--- -If combinator with state ---';
SELECT uniqMerge(s) FROM (
    SELECT uniqStateIf(number, number % 2 = 0) AS s FROM numbers(100));

SELECT '--- finalizeAggregation on state column ---';
SELECT finalizeAggregation(s) FROM (
    SELECT uniqState(number) AS s FROM numbers(100));

SELECT '--- initializeAggregation ---';
SELECT uniqMerge(initializeAggregation('uniqState', 42::UInt64));

SELECT '--- state in subquery: nested expressions ---';
SELECT uniqMerge(s) FROM (
    SELECT uniqState(x) AS s FROM (SELECT arrayJoin([1, 2, 3, 2, 1]) AS x));

DROP TABLE agg_states;
