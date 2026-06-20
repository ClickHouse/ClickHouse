-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- EXPLAIN output may differ

SET max_rows_to_group_by = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test (rn UInt64, k UInt64, v Int64, arr Array(Int64)) ENGINE = MergeTree ORDER BY rn;

-- Single dominant hot key (0, ~90%) over a high-cardinality cold tail.
INSERT INTO test
SELECT number AS rn, (number % 10 < 9) ? 0 : (number % 200000) AS k, toInt64(number) AS v, [toInt64(number % 3), toInt64(number % 7)] AS arr
FROM numbers(2000000);

SELECT '-If and -Array combinators';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT k, sumIf(v, rn % 3 = 0) AS s1, countIf(rn % 2 = 0) AS s2, sumArray(arr) AS s3
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT k, sumIf(v, rn % 3 = 0) AS s1, countIf(rn % 2 = 0) AS s2, sumArray(arr) AS s3
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1));

-- -State combinators flow through the residue + hot merge as partial states; finalize before comparing,
-- since raw state bytes can differ by merge order.
SELECT '-State combinators (finalized)';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT k, finalizeAggregation(su) AS s1, finalizeAggregation(uu) AS s2, finalizeAggregation(am) AS s3 FROM
            (SELECT k, sumState(v) AS su, uniqExactState(v) AS uu, argMinState(v, rn) AS am
             FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0)))
    =
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT k, finalizeAggregation(su) AS s1, finalizeAggregation(uu) AS s2, finalizeAggregation(am) AS s3 FROM
            (SELECT k, sumState(v) AS su, uniqExactState(v) AS uu, argMinState(v, rn) AS am
             FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1)));

SELECT 'Pipeline contains skew-split transforms';
SELECT countIf(explain LIKE '%BufferedScatterTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, sumState(v) FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1);

DROP TABLE test;
