-- When a join is sharded by primary-key ranges, the data goes through per-shard join clones.
-- `EXPLAIN ANALYZE` must aggregate the statistics over the shards: the values cover the whole
-- input and must not depend on the number of threads.

SET enable_analyzer = 1;
-- EXPLAIN ANALYZE rejects distributed plans.
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET join_use_nulls = 0;
-- The sharding needs a plain hash join without automatic spilling to disk,
-- and reads both sides in primary-key order.
SET query_plan_join_shard_by_pk_ranges = 1;
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET optimize_read_in_order = 1;

DROP TABLE IF EXISTS left_side_05055;
DROP TABLE IF EXISTS right_side_05055;

-- Small granules, so the reads can be split at primary-key borders.
CREATE TABLE left_side_05055 (k UInt8, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE right_side_05055 (k UInt8, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;

INSERT INTO left_side_05055 SELECT number % 200, number FROM numbers(1000);
INSERT INTO right_side_05055 SELECT number % 200, number FROM numbers(1000);

-- The join is sharded: both sides are read in primary-key order and split at the borders.
SELECT 'sharding applies';
SELECT DISTINCT 1 FROM (EXPLAIN PIPELINE
    SELECT count() FROM left_side_05055 AS l ALL INNER JOIN right_side_05055 AS r ON l.k = r.k
    SETTINGS join_algorithm = 'hash', max_threads = 2)
WHERE explain LIKE '%FilterSortedStreamByRange%';

SELECT 'ground truth: unique right keys';
SELECT uniqExact(k) FROM right_side_05055;

SELECT 'hash, max_threads = 2';
SELECT maxIf(extract(explain, 'unique keys ([0-9.]+)'), explain LIKE '%Hash table:%') AS unique_keys
FROM (EXPLAIN ANALYZE SELECT count() FROM left_side_05055 AS l ALL INNER JOIN right_side_05055 AS r ON l.k = r.k
      SETTINGS join_algorithm = 'hash', max_threads = 2);

SELECT 'parallel_hash, max_threads = 16';
SELECT maxIf(extract(explain, 'unique keys ([0-9.]+)'), explain LIKE '%Hash table:%') AS unique_keys
FROM (EXPLAIN ANALYZE SELECT count() FROM left_side_05055 AS l ALL INNER JOIN right_side_05055 AS r ON l.k = r.k
      SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 16);

DROP TABLE left_side_05055;
DROP TABLE right_side_05055;
