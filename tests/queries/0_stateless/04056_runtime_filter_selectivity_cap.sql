-- Tags: no-random-settings, no-random-merge-tree-settings

SET allow_statistics_optimize = 1;
SET allow_experimental_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET join_algorithm = 'hash';
SET join_runtime_bloom_filter_bytes = 128; -- 1024 bits
SET join_runtime_filter_exact_values_limit = 0; -- Force Bloom filter usage
SET join_runtime_bloom_filter_max_estimated_ratio_of_set_bits = 0.9;

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_gate;

CREATE TABLE t_probe (id UInt64, payload String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_build (id UInt64, tag String) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE t_gate (id UInt64) ENGINE = MergeTree() ORDER BY id;

INSERT INTO t_probe SELECT number, 'x' FROM numbers(100000);
INSERT INTO t_build SELECT number, 'y' FROM numbers(100000);
INSERT INTO t_gate SELECT number FROM numbers(20);

OPTIMIZE TABLE t_probe FINAL;
OPTIMIZE TABLE t_build FINAL;
OPTIMIZE TABLE t_gate FINAL;

-- Warm up metadata/statistics reads.
SELECT count() FROM t_probe FORMAT Null;
SELECT count() FROM t_build FORMAT Null;
SELECT count() FROM t_gate FORMAT Null;
SELECT uniqExact(id) FROM t_build FORMAT Null;

-- Case 1: Baseline large build side -> high saturation -> `RuntimeFilter` should be disabled.
SELECT '--- Baseline Saturation (Disabled) ---';
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM
(
    EXPLAIN PLAN
    SELECT count()
    FROM t_probe p
    INNER JOIN t_build b ON p.id = b.id
);

-- Case 2: Build-side `WHERE` filter (no `LIMIT`) should cap rows/`NDV` and keep `RuntimeFilter` enabled.
SELECT '--- Filtered Build Side (Enabled) ---';
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM
(
    EXPLAIN PLAN
    SELECT count()
    FROM t_probe p
    INNER JOIN (SELECT id FROM t_build WHERE id < 20) b ON p.id = b.id
);

-- Case 3: Build side produced by an upstream `JOIN` should use small join output estimate and keep `RuntimeFilter` enabled.
SELECT '--- Upstream Join Build Side (Enabled) ---';
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM
(
    EXPLAIN PLAN
    SELECT count()
    FROM t_probe p
    INNER JOIN
    (
        SELECT b.id
        FROM t_build b
        INNER JOIN t_gate g ON b.id = g.id
    ) x ON p.id = x.id
);

DROP TABLE t_probe;
DROP TABLE t_build;
DROP TABLE t_gate;
