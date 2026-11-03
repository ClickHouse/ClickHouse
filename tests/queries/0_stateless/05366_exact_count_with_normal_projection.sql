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

-- The same for granules counted from the query condition cache, which cut the read the same way.

DROP TABLE IF EXISTS t_exact_count_normal_projection_qcc;

CREATE TABLE t_exact_count_normal_projection_qcc (k UInt64, s String, PROJECTION p (SELECT s ORDER BY s))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 100000, index_granularity_bytes = '10Mi';

SYSTEM STOP MERGES t_exact_count_normal_projection_qcc;

-- Granule 0 has too many matching rows to be counted, granules 1 and 2 have 10 each.
INSERT INTO t_exact_count_normal_projection_qcc
SELECT number, if(number < 100000 OR number % 10000 = 5, 'needle', 'hay') FROM numbers(300000);

-- Records the counts of granules 1 and 2.
SELECT count() FROM t_exact_count_normal_projection_qcc PREWHERE s LIKE '%needle%'
SETTINGS use_query_condition_cache = 1, use_columns_cache = 0, enable_parallel_replicas = 0;

-- Granules 1 and 2 are counted, granule 0 is read from the table, not from `p`. EXPLAIN uses the query condition
-- cache only when the explained query enables it.
SELECT
    countIf(explain LIKE '%ReadFromPreparedSource (_exact_count_projection)%') > 0,
    countIf(explain LIKE '%ReadFromMergeTree (p)%') > 0
FROM (EXPLAIN SELECT count() FROM t_exact_count_normal_projection_qcc PREWHERE s LIKE '%needle%'
    SETTINGS use_query_condition_cache = 1, use_columns_cache = 0, enable_parallel_replicas = 0, prefer_optimize_projection = 1);

SELECT count() FROM t_exact_count_normal_projection_qcc PREWHERE s LIKE '%needle%'
SETTINGS use_query_condition_cache = 1, use_columns_cache = 0, enable_parallel_replicas = 0, prefer_optimize_projection = 1;
SELECT countIf(s LIKE '%needle%') FROM t_exact_count_normal_projection_qcc;

DROP TABLE t_exact_count_normal_projection_qcc;
