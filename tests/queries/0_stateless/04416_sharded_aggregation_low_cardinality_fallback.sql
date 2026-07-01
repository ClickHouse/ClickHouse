-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- EXPLAIN output may differ

-- Low cardinality relative to row count: sharded aggregation detects this during warmup and falls back to
-- routing every row to the hot path (default aggregation), instead of scattering. This must not change
-- results. We force multiple streams (several parts + max_threads) so the per-stream warmup decision and
-- the cross-stream merge are exercised, and compare enable_sharding_aggregator 0 vs 1.

SET max_rows_to_group_by = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test (k UInt64, v Int64) ENGINE = MergeTree ORDER BY tuple();

-- Four inserts => multiple parts => multiple streams. Only 256 distinct keys over 8M rows: low cardinality.
INSERT INTO test SELECT number % 256 AS k, toInt64(number) AS v FROM numbers(2000000);
INSERT INTO test SELECT number % 256 AS k, toInt64(number) AS v FROM numbers(2000000);
INSERT INTO test SELECT number % 256 AS k, toInt64(number) AS v FROM numbers(2000000);
INSERT INTO test SELECT number % 256 AS k, toInt64(number) AS v FROM numbers(2000000);

SELECT 'Cheap aggregates under low cardinality (fallback path)';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, min(v) AS s3, max(v) AS s4
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, min(v) AS s3, max(v) AS s4
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1));

SELECT 'Large-state aggregate under low cardinality (stays sharded)';
SELECT
    (SELECT sum(s), count() FROM (SELECT k, uniqExact(v) AS s FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT k, uniqExact(v) AS s FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1));

SELECT 'Multi-key low cardinality';
SELECT
    (SELECT sum(s), count() FROM (SELECT k, k % 7 AS k2, sum(v) AS s FROM test GROUP BY k, k2 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT k, k % 7 AS k2, sum(v) AS s FROM test GROUP BY k, k2 SETTINGS enable_sharding_aggregator = 1));

SELECT 'WITH TOTALS';
SELECT
    (SELECT sum(s), count() FROM (SELECT k, sum(v) AS s FROM test GROUP BY k WITH TOTALS SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT k, sum(v) AS s FROM test GROUP BY k WITH TOTALS SETTINGS enable_sharding_aggregator = 1));

SELECT 'Result row count is the true cardinality';
SELECT count() = 256 FROM (SELECT k FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1);

SELECT 'Pipeline still sharded (fallback is runtime routing, not a plan change)';
SELECT
    countIf(explain LIKE '%BufferedShardingTransform%') > 0,
    countIf(explain LIKE '%ColdShardAggregatingTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, count() FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1);

DROP TABLE test;
