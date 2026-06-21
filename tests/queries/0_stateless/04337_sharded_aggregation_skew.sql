-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- EXPLAIN output may differ

SET max_rows_to_group_by = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test (k UInt64, v Int64) ENGINE = MergeTree ORDER BY tuple();

-- Heavy skew: key 0 is ~90% of rows; the remaining 10% spread over a high-cardinality tail.
INSERT INTO test
SELECT (number % 10 = 0) ? (number % 200000) : 0 AS k, number AS v
FROM numbers(2000000);

SELECT 'Single key, large-state aggregates under skew';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), sum(s5), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3, min(v) AS s4, max(v) AS s5
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), sum(s5), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3, min(v) AS s4, max(v) AS s5
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1));

SELECT 'quantileExact under skew';
SELECT
    (SELECT sum(s), count() FROM (SELECT k, quantileExact(0.9)(v) AS s FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT k, quantileExact(0.9)(v) AS s FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1));

SELECT 'Multi-key GROUP BY';
SELECT
    (SELECT sum(s), count() FROM (SELECT k, k % 7 AS k2, sum(v) AS s FROM test GROUP BY k, k2 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT k, k % 7 AS k2, sum(v) AS s FROM test GROUP BY k, k2 SETTINGS enable_sharding_aggregator = 1));

SELECT 'Pipeline contains skew-split transforms';
SELECT
    countIf(explain LIKE '%BufferedShardingTransform%') > 0,
    countIf(explain LIKE '%FinalizeAggregatedTransform%') > 0,
    countIf(explain LIKE '%MergingAggregatedTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, count() FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1);

DROP TABLE test;
