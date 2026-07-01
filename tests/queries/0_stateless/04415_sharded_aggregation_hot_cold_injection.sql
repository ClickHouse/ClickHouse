-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- EXPLAIN output may differ

-- Exercises the hot->cold injection path: a hot key detected mid-scan leaves residue in its cold shard,
-- and the same hot key is aggregated in several per-stream hot shards. The cold shard must combine its
-- residue with all of those hot states (n-ary merge) and finalize the key exactly once. We force multiple
-- streams (several parts + max_threads) and check equality against the non-sharded oracle.

SET max_rows_to_group_by = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test (rn UInt64, k UInt64, ks String, kn Nullable(UInt64), v Int64) ENGINE = MergeTree ORDER BY rn;

-- Four separate inserts => at least four parts => multiple streams. In every part the hot keys (k = 0 and
-- k = 7777777) are absent from the first stretch and only become dense afterwards, so they are detected
-- mid-scan in each stream and leave residue in their cold shards.
INSERT INTO test SELECT number AS rn, if(number < 200000, number + 1, intDiv(number, 1000000) % 2 = 0 ? 0 : 7777777) AS k,
    toString(k) AS ks, if(number % 5 = 0, NULL, k) AS kn, toInt64(number) AS v FROM numbers(2000000);
INSERT INTO test SELECT number AS rn, if(number < 200000, number + 1, intDiv(number, 1000000) % 2 = 0 ? 0 : 7777777) AS k,
    toString(k) AS ks, if(number % 5 = 0, NULL, k) AS kn, toInt64(number) AS v FROM numbers(2000000);
INSERT INTO test SELECT number AS rn, if(number < 200000, number + 1, intDiv(number, 1000000) % 2 = 0 ? 0 : 7777777) AS k,
    toString(k) AS ks, if(number % 5 = 0, NULL, k) AS kn, toInt64(number) AS v FROM numbers(2000000);
INSERT INTO test SELECT number AS rn, if(number < 200000, number + 1, intDiv(number, 1000000) % 2 = 0 ? 0 : 7777777) AS k,
    toString(k) AS ks, if(number % 5 = 0, NULL, k) AS kn, toInt64(number) AS v FROM numbers(2000000);

SELECT 'UInt64 key, multiple aggregates';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), sum(s5), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3, argMin(v, rn) AS s4, quantileExact(0.9)(v) AS s5
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), sum(s5), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3, argMin(v, rn) AS s4, quantileExact(0.9)(v) AS s5
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1));

SELECT 'String key';
SELECT
    (SELECT sum(s), count() FROM (SELECT ks, sum(v) AS s FROM test GROUP BY ks SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT ks, sum(v) AS s FROM test GROUP BY ks SETTINGS enable_sharding_aggregator = 1));

SELECT 'Nullable key (hot NULLs)';
SELECT
    (SELECT sum(s), count() FROM (SELECT kn, sum(v) AS s FROM test GROUP BY kn SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT kn, sum(v) AS s FROM test GROUP BY kn SETTINGS enable_sharding_aggregator = 1));

SELECT 'Multi-key GROUP BY';
SELECT
    (SELECT sum(s), count() FROM (SELECT k, k % 13 AS k2, sum(v) AS s FROM test GROUP BY k, k2 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT k, k % 13 AS k2, sum(v) AS s FROM test GROUP BY k, k2 SETTINGS enable_sharding_aggregator = 1));

SELECT 'WITH TOTALS';
SELECT
    (SELECT sum(s), count() FROM (SELECT k, sum(v) AS s FROM test GROUP BY k WITH TOTALS SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT k, sum(v) AS s FROM test GROUP BY k WITH TOTALS SETTINGS enable_sharding_aggregator = 1));

SELECT 'Hot key 0 finalized exactly once (vs independent oracle)';
SELECT
    c = (SELECT 4 * count() FROM numbers(2000000) WHERE number >= 200000 AND intDiv(number, 1000000) % 2 = 0),
    s = (SELECT 4 * sum(toInt64(number)) FROM numbers(2000000) WHERE number >= 200000 AND intDiv(number, 1000000) % 2 = 0)
FROM (SELECT k, count() AS c, sum(v) AS s FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1)
WHERE k = 0;

SELECT 'Pipeline: cold-shard injection, no global merge';
SELECT
    countIf(explain LIKE '%BufferedShardingTransform%') > 0,
    countIf(explain LIKE '%ColdShardAggregatingTransform%') > 0,
    countIf(explain LIKE '%MergingAggregatedTransform%') = 0
FROM (EXPLAIN PIPELINE SELECT k, count() FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1);

DROP TABLE test;
