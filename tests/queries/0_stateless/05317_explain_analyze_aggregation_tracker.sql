-- Tags: no-parallel-replicas
-- - no-parallel-replicas: EXPLAIN ANALYZE rejects plans that stay distributed

-- Track aggregation memory under the same group during pipeline construction and execution.
SELECT 'single_thread', countIf(match(explain, 'Memory: bytes [1-9]')) = 1
FROM (EXPLAIN ANALYZE SELECT number % 1000 AS k, uniqExact(number) FROM numbers_mt(100000) GROUP BY k
    SETTINGS max_threads = 1);

SELECT 'parallel', countIf(match(explain, 'Memory: bytes [1-9]')) = 1
FROM (EXPLAIN ANALYZE SELECT number % 1000 AS k, uniqExact(number) FROM numbers_mt(100000) GROUP BY k
    SETTINGS max_threads = 4);

-- Scalar aggregation has no hash table, but its states are still tracked.
-- count_distinct_optimization would rewrite it into count() over GROUP BY number and add a second Aggregating step.
SELECT 'scalar', countIf(match(explain, 'Memory: bytes [1-9]')) = 1
FROM (EXPLAIN ANALYZE SELECT uniqExact(number) FROM numbers_mt(100000)
    SETTINGS count_distinct_optimization = 0);

-- Parts that predate the projection are aggregated from scratch, so the projection step tracks them.
DROP TABLE IF EXISTS t_agg_proj;
CREATE TABLE t_agg_proj (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_agg_proj SELECT number % 1000, number FROM numbers(100000);
ALTER TABLE t_agg_proj ADD PROJECTION p (SELECT k, uniqExact(v) GROUP BY k);
INSERT INTO t_agg_proj SELECT number % 1000, number FROM numbers(100000);
SELECT 'projection', countIf(explain LIKE '%AggregatingProjection%') = 1, countIf(match(explain, 'Memory: bytes [1-9]')) = 1
FROM (EXPLAIN ANALYZE SELECT k, uniqExact(v) FROM t_agg_proj GROUP BY k
    SETTINGS optimize_use_projections = 1, force_optimize_projection = 1);
DROP TABLE t_agg_proj;
