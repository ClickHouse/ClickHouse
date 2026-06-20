-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- EXPLAIN output may differ

SET max_rows_to_group_by = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test (rn UInt64, k UInt64, v Int64) ENGINE = MergeTree ORDER BY rn;

-- Four co-dominant keys 0..3 (~22% each) over a high-cardinality cold tail, so several keys are promoted
-- to the hot path at once (with max_threads = 8 a key must exceed ~1/8 of the rows to be hot).
INSERT INTO test
SELECT number AS rn, (number % 9 < 8) ? (number % 4) : (1000000 + number % 500000) AS k, toInt64(number) AS v
FROM numbers(4000000);

SELECT 'Multiple hot keys, single-key GROUP BY';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3, argMin(v, rn) AS s4
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, uniqExact(v) AS s3, argMin(v, rn) AS s4
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1));

SELECT 'Multiple hot keys, multi-key GROUP BY';
SELECT
    (SELECT sum(s1), sum(s2), count() FROM
        (SELECT k, k % 7 AS k2, count() AS s1, sum(v) AS s2 FROM test GROUP BY k, k2 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), count() FROM
        (SELECT k, k % 7 AS k2, count() AS s1, sum(v) AS s2 FROM test GROUP BY k, k2 SETTINGS enable_sharding_aggregator = 1));

SELECT 'Pipeline contains skew-split transforms';
SELECT countIf(explain LIKE '%BufferedScatterTransform%') > 0
FROM (EXPLAIN PIPELINE SELECT k, count() FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1);

DROP TABLE test;
