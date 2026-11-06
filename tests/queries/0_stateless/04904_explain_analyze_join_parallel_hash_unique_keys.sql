-- `unique keys` of a `parallel_hash` join is the sum over the per-slot hash tables when they are
-- one-level, which a key narrower than 32 bits gives. The value counts the whole right table and so
-- must not depend on the number of slots.

DROP TABLE IF EXISTS left_side;
DROP TABLE IF EXISTS right_side;
SET enable_analyzer = 1;
-- EXPLAIN ANALYZE rejects distributed plans.
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET join_use_nulls = 0;

DROP TABLE IF EXISTS left_side;
DROP TABLE IF EXISTS right_side;

CREATE TABLE left_side (k UInt8, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE right_side (k UInt8, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO left_side SELECT number % 200, number FROM numbers(1000);
INSERT INTO right_side SELECT number % 200, number FROM numbers(1000);

SELECT 'ground truth: unique right keys';
SELECT uniqExact(k) FROM right_side;

SELECT 'max_threads = 2';
SELECT maxIf(extract(explain, 'unique keys ([0-9.]+)'), explain LIKE '%Hash table:%') AS unique_keys
FROM (EXPLAIN ANALYZE SELECT count() FROM left_side AS l ALL INNER JOIN right_side AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 2);

SELECT 'max_threads = 16';
SELECT maxIf(extract(explain, 'unique keys ([0-9.]+)'), explain LIKE '%Hash table:%') AS unique_keys
FROM (EXPLAIN ANALYZE SELECT count() FROM left_side AS l ALL INNER JOIN right_side AS r ON l.k = r.k SETTINGS join_algorithm = 'parallel_hash', parallel_hash_join_threshold = 1, max_threads = 16);

DROP TABLE left_side;
DROP TABLE right_side;
