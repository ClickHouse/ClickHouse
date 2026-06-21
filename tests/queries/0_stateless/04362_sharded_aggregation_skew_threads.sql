-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- EXPLAIN output may differ

SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS test;
CREATE TABLE test (rn UInt64, k UInt64, v Int64) ENGINE = MergeTree ORDER BY rn;

-- Eight co-dominant keys 0..7 (~11% each) over a high-cardinality cold tail. The promotion threshold and
-- shard count both scale with max_threads, so the result must match the normal path at every thread count
-- (we assert only that, never which keys were promoted).
INSERT INTO test
SELECT number AS rn, (number % 9 < 8) ? (number % 8) : (1000000 + number % 100000) AS k, toInt64(number) AS v
FROM numbers(4000000);

SELECT 'max_threads = 2';
SELECT (SELECT sum(s1), sum(s2), sum(s3), count() FROM (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3 FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0, max_threads = 2))
     = (SELECT sum(s1), sum(s2), sum(s3), count() FROM (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3 FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1, max_threads = 2));

SELECT 'max_threads = 4';
SELECT (SELECT sum(s1), sum(s2), sum(s3), count() FROM (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3 FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0, max_threads = 4))
     = (SELECT sum(s1), sum(s2), sum(s3), count() FROM (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3 FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1, max_threads = 4));

SELECT 'max_threads = 16';
SELECT (SELECT sum(s1), sum(s2), sum(s3), count() FROM (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3 FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0, max_threads = 16))
     = (SELECT sum(s1), sum(s2), sum(s3), count() FROM (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3 FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1, max_threads = 16));

SELECT 'max_threads = 32';
SELECT (SELECT sum(s1), sum(s2), sum(s3), count() FROM (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3 FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0, max_threads = 32))
     = (SELECT sum(s1), sum(s2), sum(s3), count() FROM (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3 FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1, max_threads = 32));

SELECT 'max_threads = 64';
SELECT (SELECT sum(s1), sum(s2), sum(s3), count() FROM (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3 FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0, max_threads = 64))
     = (SELECT sum(s1), sum(s2), sum(s3), count() FROM (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3 FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1, max_threads = 64));

SELECT 'Pipeline engages at a high thread count';
SELECT countIf(explain LIKE '%BufferedShardingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, count() FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1, max_threads = 64);

DROP TABLE test;
