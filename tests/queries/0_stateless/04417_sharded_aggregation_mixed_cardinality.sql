-- Mixed cardinality across parts: some streams see a low-cardinality slice (and fall back, routing
-- everything to the merge path) while others see a high-cardinality slice (and keep scattering). The key
-- ranges OVERLAP (0..255 appear in both), so a key can arrive at its cold shard both as scattered residue
-- (from a scattering stream) and as an injected merge-path state (from a fallen-back stream). The cold
-- shard must combine them and finalize once. Per-stream stream/part assignment is not controllable, so
-- this does not guarantee the mix, but it stresses the cross-mode merge; results must match non-sharded.

SET max_rows_to_group_by = 0;
SET max_rows_to_read = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test (k UInt64, v Int64) ENGINE = MergeTree ORDER BY tuple();

-- Two low-cardinality parts (256 keys) and two high-cardinality parts (5M keys); keys 0..255 overlap.
INSERT INTO test SELECT number % 256 AS k, toInt64(number) AS v FROM numbers(4000000);
INSERT INTO test SELECT number % 5000000 AS k, toInt64(number) AS v FROM numbers(4000000);
INSERT INTO test SELECT number % 256 AS k, toInt64(number) AS v FROM numbers(4000000);
INSERT INTO test SELECT number % 5000000 AS k, toInt64(number) AS v FROM numbers(4000000);

SELECT 'Cheap aggregates, mixed cardinality';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, min(v) AS s3, max(v) AS s4
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), sum(s4), count() FROM
        (SELECT k, count() AS s1, sum(v) AS s2, min(v) AS s3, max(v) AS s4
         FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1));

SELECT 'Large-state aggregate, mixed cardinality';
SELECT
    (SELECT sum(s), count() FROM (SELECT k, uniqExact(v) AS s FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT k, uniqExact(v) AS s FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1));

SELECT 'Overlapping key finalized once (vs independent oracle)';
SELECT
    c = (SELECT 2 * count() FROM numbers(4000000) WHERE number % 256 = 5)
      + (SELECT 2 * count() FROM numbers(4000000) WHERE number % 5000000 = 5)
FROM (SELECT k, count() AS c FROM test GROUP BY k SETTINGS enable_sharding_aggregator = 1)
WHERE k = 5;

DROP TABLE test;
