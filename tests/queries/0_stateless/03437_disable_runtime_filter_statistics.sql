-- Tags: no-random-settings, no-random-merge-tree-settings
-- Disable randomized settings because this test asserts exact EXPLAIN PLAN outputs 
-- for join runtime filters. Randomizing parallel replicas, join swapping, or memory 
-- limits alters the fundamental query plan and causes the fallback heuristics to flake.

SET allow_statistics_optimize = 1;
SET allow_experimental_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET join_runtime_bloom_filter_bytes = 128;
SET join_runtime_filter_exact_values_limit = 0;

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_no_stats;

CREATE TABLE t_probe (id UInt64, payload String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_build (id UInt64, tag String) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t_probe SELECT number, 'x' FROM numbers(100000);
INSERT INTO t_build SELECT number, 'y' FROM numbers(100000);

OPTIMIZE TABLE t_probe FINAL;
OPTIMIZE TABLE t_build FINAL;

-- WARM-UP: Force storage metadata into the optimizer's view silently
SELECT count() FROM t_probe FORMAT Null;
SELECT count() FROM t_build FORMAT Null;
SELECT uniqExact(id) FROM t_build FORMAT Null;

-- Test Case 1: PK/FK Saturation (Expect DISABLED)
-- 100k rows in 128 bytes results in p=1.0. Filter should be disabled.
SET join_runtime_filter_build_saturation_threshold = 0.9;
SELECT '--- PK/FK Saturation (Disabled) ---';
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (
    EXPLAIN PLAN
    SELECT p.payload FROM t_probe p
    INNER JOIN t_build b ON p.id = b.id
);

-- Test Case 2: Selective Build Side (Expect ENABLED)
-- Subquery LIMIT 100 ensures the optimizer sees a small 'n'. Filter should be enabled.
SELECT '--- Selective Build Side (Enabled) ---';
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (
    EXPLAIN PLAN
    SELECT p.payload FROM t_probe p
    INNER JOIN (SELECT id FROM t_build LIMIT 100) b ON p.id = b.id
);

-- Test Case 3: Missing Statistics Fallback (Expect ENABLED)
-- Memory tables often have zero statistics; logic should default to keeping filter ON.
CREATE TABLE t_no_stats (id UInt64) ENGINE = Memory;
INSERT INTO t_no_stats VALUES (1), (2), (3);
SELECT count() FROM t_no_stats FORMAT Null;

SELECT '--- Missing Statistics Fallback (Enabled) ---';
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (
    EXPLAIN PLAN
    SELECT p.payload FROM t_probe p
    INNER JOIN t_no_stats n ON p.id = n.id
);

-- Test Case 4: Threshold Boundary (Expect ENABLED)
-- Setting threshold to 1.0 bypasses the saturation check.
SET join_runtime_filter_build_saturation_threshold = 1.0;
SELECT '--- Threshold Boundary (Enabled) ---';
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (
    EXPLAIN PLAN
    SELECT p.payload FROM t_probe p
    INNER JOIN t_build b ON p.id = b.id
);

DROP TABLE t_probe;
DROP TABLE t_build;
DROP TABLE IF EXISTS t_no_stats;