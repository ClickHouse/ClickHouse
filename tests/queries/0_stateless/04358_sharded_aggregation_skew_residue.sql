-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- EXPLAIN output may differ

SET max_rows_to_group_by = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test (rn UInt64, k UInt64, v Int64) ENGINE = MergeTree ORDER BY rn;

-- The hot key 7777777 is absent from the first third of the scan and only becomes dense afterwards, so
-- it is detected mid-scan and leaves a large residue across the cold shards (rows seen before detection).
INSERT INTO test
SELECT number AS rn, if(number < 1000000, number, (number % 2 = 0) ? 7777777 : number) AS k, toInt64(number) AS v
FROM numbers(3000000);

SELECT 'Argument-bearing aggregates under large residue';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), sum(s5), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3, argMin(v, rn) AS s4, sumIf(v, rn % 3 = 0) AS s5
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), sum(s5), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3, argMin(v, rn) AS s4, sumIf(v, rn % 3 = 0) AS s5
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1));

SELECT 'Hot key finalized exactly once (vs independent oracle)';
SELECT
    c = (SELECT count() FROM numbers(3000000) WHERE number >= 1000000 AND number % 2 = 0),
    s = (SELECT sum(toInt64(number)) FROM numbers(3000000) WHERE number >= 1000000 AND number % 2 = 0)
FROM (SELECT k, count() AS c, sum(v) AS s FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1)
WHERE k = 7777777;

SELECT 'Pipeline contains skew-split transforms';
SELECT
    countIf(explain LIKE '%BufferedShardingTransform%') > 0,
    countIf(explain LIKE '%ColdShardAggregatingTransform%') > 0,
    countIf(explain LIKE '%MergingAggregatedTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT k, count() FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1);

DROP TABLE test;
