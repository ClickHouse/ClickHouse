-- Tags: no-random-settings, no-random-merge-tree-settings
-- Disable randomized settings because this test relies on exact table statistics 
-- (row counts) and a deterministic execution plan to calculate Bloom filter saturation. 
-- Randomizing memory limits, block sizes, join swapping, or join algorithms will alter 
-- the EXPLAIN output and cause the threshold math to flake.

SET allow_statistics_optimize = 1;
SET allow_experimental_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_bloom_filter_bytes = 128; -- 1024 bits
SET join_runtime_filter_exact_values_limit = 0; -- Force Bloom filter usage

DROP TABLE IF EXISTS t_left;
DROP TABLE IF EXISTS t_right;

CREATE TABLE t_left (id UInt64, val String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE t_right (id UInt64, val String) ENGINE = MergeTree ORDER BY id;

INSERT INTO t_left SELECT number, 'val' FROM numbers(1000);
INSERT INTO t_right SELECT number, 'val' FROM numbers(1000);

OPTIMIZE TABLE t_left FINAL;
OPTIMIZE TABLE t_right FINAL;

-- WARM-UP: Force ClickHouse to load metadata and cardinality into the optimizer cache
-- We use FORMAT Null to prevent these numbers from appearing in the test output.
SELECT count() FROM t_left FORMAT Null;
SELECT count() FROM t_right FORMAT Null;
SELECT uniqExact(id) FROM t_right FORMAT Null;

-- Case 1: Saturation High (Expect DISABLED)
-- n=1000, k=3, m=1024 bits => p ≈ 0.946. Since 0.946 >= 0.9, the filter should be skipped.
SET join_runtime_filter_build_saturation_threshold = 0.9;
SELECT '--- Expect NO Runtime Filter (Saturation High) ---';
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (EXPLAIN PLAN SELECT count() FROM t_left JOIN t_right ON t_left.id = t_right.id);

-- Case 2: Threshold Bypass (Expect ENABLED)
-- Threshold 1.0 means we ignore saturation logic, so the filter should be created.
SET join_runtime_filter_build_saturation_threshold = 1.0;
SELECT '--- Expect Runtime Filter (Check Disabled) ---';
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (EXPLAIN PLAN SELECT count() FROM t_left JOIN t_right ON t_left.id = t_right.id);

-- Case 3: Saturation Low (Expect ENABLED)
-- Using a subquery with LIMIT 10 forces the optimizer to see n=10.
-- p = 1 - exp(-30/1024) ≈ 0.029. Since 0.029 < 0.9, the filter should stay enabled.
SET join_runtime_filter_build_saturation_threshold = 0.9;
SELECT '--- Expect Runtime Filter (Saturation Low) ---';
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (EXPLAIN PLAN SELECT count() FROM t_left JOIN (SELECT id FROM t_right LIMIT 10) as r ON t_left.id = r.id);

DROP TABLE t_left;
DROP TABLE t_right;
