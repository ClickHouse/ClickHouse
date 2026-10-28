-- Random settings limits: optimize_use_projections=(1, None); optimize_use_implicit_projections=(1, None)

-- The exact count optimization counts fully matching granules from the primary key and reads only the rest of
-- each part; such a part must not be served from a normal projection, which would return those rows again.

DROP TABLE IF EXISTS t_exact_count_normal_projection;

CREATE TABLE t_exact_count_normal_projection (d UInt8, s UInt8, pad String, PROJECTION p (SELECT d, s ORDER BY d, s))
ENGINE = MergeTree ORDER BY (d, s) SETTINGS index_granularity = 8192, index_granularity_bytes = 10000;

-- Keep the two parts apart: only the first one has exactly counted granules.
SYSTEM STOP MERGES t_exact_count_normal_projection;

INSERT INTO t_exact_count_normal_projection SELECT number % 3, number % 2, repeat('x', 100) FROM numbers(4000);
INSERT INTO t_exact_count_normal_projection SELECT number % 30, number % 2, repeat('x', 100) FROM numbers(500);

-- Exact count, the first part read from the table, the second part still read from `p`.
SELECT
    countIf(explain LIKE '%ReadFromPreparedSource (_exact_count_projection)%') > 0,
    countIf(explain LIKE '%ReadFromMergeTree (%t_exact_count_normal_projection)%') > 0,
    countIf(explain LIKE '%ReadFromMergeTree (p)%') > 0
FROM (EXPLAIN SELECT count() FROM t_exact_count_normal_projection WHERE s = 1)
SETTINGS enable_parallel_replicas = 0;

SELECT count() FROM t_exact_count_normal_projection WHERE s = 1;
SELECT countIf(s = 1) FROM t_exact_count_normal_projection;

SYSTEM START MERGES t_exact_count_normal_projection;
OPTIMIZE TABLE t_exact_count_normal_projection FINAL;

-- The merged part has exactly counted granules, so `p` is not read.
SELECT
    countIf(explain LIKE '%ReadFromPreparedSource (_exact_count_projection)%') > 0,
    countIf(explain LIKE '%ReadFromMergeTree (%t_exact_count_normal_projection)%') > 0,
    countIf(explain LIKE '%ReadFromMergeTree (p)%') > 0
FROM (EXPLAIN SELECT count() FROM t_exact_count_normal_projection WHERE s = 1)
SETTINGS enable_parallel_replicas = 0;

SELECT count() FROM t_exact_count_normal_projection WHERE s = 1;

DROP TABLE t_exact_count_normal_projection;
