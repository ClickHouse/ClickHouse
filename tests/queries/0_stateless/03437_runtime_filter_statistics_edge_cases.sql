-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- This test asserts exact EXPLAIN PLAN runtime-filter presence for planning-time
-- statistics based disabling. Random join swapping, parallel replicas, or random
-- MergeTree/statistics settings can change the plan shape or estimates.

SET allow_statistics_optimize = 1;
SET allow_experimental_analyzer = 1;
SET enable_join_runtime_filters = 1;
SET join_algorithm = 'hash';
SET optimize_move_to_prewhere = 0;
SET query_plan_optimize_join_order_limit = 0;

DROP TABLE IF EXISTS rf_edge_probe_u64;
DROP TABLE IF EXISTS rf_edge_build_u64_1k;
DROP TABLE IF EXISTS rf_edge_build_u64_stats_100k;
DROP TABLE IF EXISTS rf_edge_build_u64_no_stats_100k;
DROP TABLE IF EXISTS rf_edge_probe_str;
DROP TABLE IF EXISTS rf_edge_build_str_1k;
DROP TABLE IF EXISTS rf_edge_probe_nullable;
DROP TABLE IF EXISTS rf_edge_build_nullable_1k;

CREATE TABLE rf_edge_probe_u64 (id UInt64, payload String) ENGINE = MergeTree() ORDER BY id SETTINGS auto_statistics_types = 'minmax,uniq';
CREATE TABLE rf_edge_build_u64_1k (id UInt64, tag String) ENGINE = MergeTree() ORDER BY id SETTINGS auto_statistics_types = 'minmax,uniq';
CREATE TABLE rf_edge_build_u64_stats_100k (id UInt64, tag String) ENGINE = MergeTree() ORDER BY id SETTINGS auto_statistics_types = 'minmax,uniq';
CREATE TABLE rf_edge_build_u64_no_stats_100k (id UInt64, tag String) ENGINE = MergeTree() ORDER BY id SETTINGS auto_statistics_types = '';
CREATE TABLE rf_edge_probe_str (id String, payload String) ENGINE = MergeTree() ORDER BY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE rf_edge_build_str_1k (id String, tag String) ENGINE = MergeTree() ORDER BY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE rf_edge_probe_nullable (id Nullable(UInt64), payload String) ENGINE = MergeTree() ORDER BY tuple() SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE rf_edge_build_nullable_1k (id Nullable(UInt64), tag String) ENGINE = MergeTree() ORDER BY tuple() SETTINGS auto_statistics_types = 'uniq';

INSERT INTO rf_edge_probe_u64 SELECT number, 'p' FROM numbers(100000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO rf_edge_build_u64_1k SELECT number, 'b' FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO rf_edge_build_u64_stats_100k SELECT number, 'b' FROM numbers(100000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO rf_edge_build_u64_no_stats_100k SELECT number, 'b' FROM numbers(100000);
INSERT INTO rf_edge_probe_str SELECT toString(number), 'p' FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO rf_edge_build_str_1k SELECT toString(number), 'b' FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO rf_edge_probe_nullable SELECT if(number % 100 = 0, NULL, number), 'p' FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;
INSERT INTO rf_edge_build_nullable_1k SELECT if(number % 100 = 0, NULL, number), 'b' FROM numbers(1000) SETTINGS materialize_statistics_on_insert = 1;

OPTIMIZE TABLE rf_edge_probe_u64 FINAL;
OPTIMIZE TABLE rf_edge_build_u64_1k FINAL;
OPTIMIZE TABLE rf_edge_build_u64_stats_100k FINAL;
OPTIMIZE TABLE rf_edge_build_u64_no_stats_100k FINAL;
OPTIMIZE TABLE rf_edge_probe_str FINAL;
OPTIMIZE TABLE rf_edge_build_str_1k FINAL;
OPTIMIZE TABLE rf_edge_probe_nullable FINAL;
OPTIMIZE TABLE rf_edge_build_nullable_1k FINAL;

-- The planner should not apply Bloom saturation math to a build side that runtime
-- will keep as an exact Set. The very low threshold would disable the filter in the
-- old implementation even though n is below the exact row/byte limits.
SELECT '--- Exact UInt64 Build Side Below Exact Limit (Enabled) ---';
SET join_runtime_bloom_filter_bytes = 524288;
SET join_runtime_filter_exact_values_limit = 10000;
SET join_runtime_bloom_filter_max_estimated_ratio_of_set_bits = 0.0001;
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (EXPLAIN PLAN SELECT count() FROM rf_edge_probe_u64 p INNER JOIN rf_edge_build_u64_1k b ON p.id = b.id);

-- Same exact-path protection for String keys.
SELECT '--- String Build Side Below Exact Limit (Enabled) ---';
SET join_runtime_bloom_filter_bytes = 524288;
SET join_runtime_filter_exact_values_limit = 10000;
SET join_runtime_bloom_filter_max_estimated_ratio_of_set_bits = 0.0001;
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (EXPLAIN PLAN SELECT count() FROM rf_edge_probe_str p INNER JOIN rf_edge_build_str_1k b ON p.id = b.id);

-- Nullable keys are not Bloom-capable here, so the planner must keep the exact runtime filter.
SELECT '--- Nullable Build Side Below Exact Limit (Enabled) ---';
SET join_runtime_bloom_filter_bytes = 524288;
SET join_runtime_filter_exact_values_limit = 10000;
SET join_runtime_bloom_filter_max_estimated_ratio_of_set_bits = 0.0001;
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (EXPLAIN PLAN SELECT count() FROM rf_edge_probe_nullable p INNER JOIN rf_edge_build_nullable_1k b ON p.id = b.id);

-- Zero-row estimates are meaningful and must not fall back to full part row count.
SELECT '--- Empty Build Predicate Estimate (Enabled) ---';
SET join_runtime_bloom_filter_bytes = 128;
SET join_runtime_filter_exact_values_limit = 0;
SET join_runtime_bloom_filter_max_estimated_ratio_of_set_bits = 0.9;
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (EXPLAIN PLAN SELECT count() FROM rf_edge_probe_u64 p INNER JOIN (SELECT id FROM rf_edge_build_u64_stats_100k WHERE id = 1000000) b ON p.id = b.id);

-- Missing MergeTree statistics must keep runtime filters enabled rather than disabling from raw part rows.
SELECT '--- MergeTree Missing Statistics (Enabled) ---';
SET join_runtime_bloom_filter_bytes = 128;
SET join_runtime_filter_exact_values_limit = 0;
SET join_runtime_bloom_filter_max_estimated_ratio_of_set_bits = 0.9;
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM (EXPLAIN PLAN SELECT count() FROM rf_edge_probe_u64 p INNER JOIN rf_edge_build_u64_no_stats_100k b ON p.id = b.id);

-- Build-side filters separated by LIMIT must be combined for the saturation estimate.
SELECT '--- Stacked Build Filters With Limit (Enabled) ---';
SET join_runtime_bloom_filter_bytes = 128;
SET join_runtime_filter_exact_values_limit = 0;
SET join_runtime_bloom_filter_max_estimated_ratio_of_set_bits = 0.9;
SELECT max(explain LIKE '%RuntimeFilter%' OR explain LIKE '%DynamicFilter%')
FROM
(
    EXPLAIN PLAN
    SELECT count()
    FROM rf_edge_probe_u64 p
    INNER JOIN
    (
        SELECT id
        FROM
        (
            SELECT id
            FROM rf_edge_build_u64_stats_100k
            WHERE id >= 10
            LIMIT 100000
        )
        WHERE id < 20
    ) b ON p.id = b.id
);

DROP TABLE rf_edge_probe_u64;
DROP TABLE rf_edge_build_u64_1k;
DROP TABLE rf_edge_build_u64_stats_100k;
DROP TABLE rf_edge_build_u64_no_stats_100k;
DROP TABLE rf_edge_probe_str;
DROP TABLE rf_edge_build_str_1k;
DROP TABLE rf_edge_probe_nullable;
DROP TABLE rf_edge_build_nullable_1k;
