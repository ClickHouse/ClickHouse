SET enable_lightweight_update = 1, apply_patch_parts = 1, optimize_trivial_count_query = 1;
-- Exercise trivial count eligibility without using explicit or implicit projections.
SET optimize_use_projections = 0, optimize_use_implicit_projections = 0;

SELECT '--- V2: value-only patches ---';

DROP TABLE IF EXISTS t_lwu_trivial_count;
CREATE TABLE t_lwu_trivial_count
(
    id UInt64,
    v Nullable(UInt64)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1,
    enable_block_offset_column = 1,
    patch_parts_version = 'v2';

-- Keep the patches pending so the queries exercise patch application during reads.
SYSTEM STOP MERGES t_lwu_trivial_count;
INSERT INTO t_lwu_trivial_count SELECT number, number FROM numbers(1000);
UPDATE t_lwu_trivial_count SET v = NULL WHERE id < 10;

-- Changing values preserves all 1000 rows and allows a metadata-only count.
SELECT count() FROM t_lwu_trivial_count;

SELECT countIf(explain LIKE '%Optimized trivial count%')
FROM (EXPLAIN SELECT count() FROM t_lwu_trivial_count);

-- Counting non-null values must apply the patch: ten values became `NULL`.
SELECT count(v) FROM t_lwu_trivial_count;

-- A filtered count must also see the ten patched values.
SELECT count() FROM t_lwu_trivial_count WHERE isNull(v);

SELECT '--- V2: pending delete patches ---';

-- Delete twenty rows through a patch, leaving the base parts without a delete mask.
DELETE FROM t_lwu_trivial_count WHERE id >= 980
SETTINGS lightweight_delete_mode = 'lightweight_update_force';

-- Verify that the delete mask exists only in patch parts.
SELECT countIf(has_lightweight_delete AND startsWith(name, 'patch')) > 0,
    countIf(has_lightweight_delete AND NOT startsWith(name, 'patch')) = 0
FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_trivial_count' AND active;

-- The pending delete reduces the count to 980 and prevents the metadata-only plan.
SELECT count() FROM t_lwu_trivial_count;

SELECT countIf(explain LIKE '%Optimized trivial count%')
FROM (EXPLAIN SELECT count() FROM t_lwu_trivial_count);

-- Ignoring patches exposes all 1000 original rows.
SELECT count() FROM t_lwu_trivial_count SETTINGS apply_patch_parts = 0;

-- Applying both patches leaves 970 non-null values: 1000 minus 20 deleted and 10 null rows.
SELECT count(v) FROM t_lwu_trivial_count;

DROP TABLE t_lwu_trivial_count;

SELECT '--- V1: value-only patches ---';

-- Legacy value-only patches must also allow a metadata-only count.
CREATE TABLE t_lwu_trivial_count
(
    id UInt64,
    v UInt64
)
ENGINE = MergeTree
ORDER BY id
SETTINGS enable_block_number_column = 1,
    enable_block_offset_column = 1,
    patch_parts_version = 'v1';

SYSTEM STOP MERGES t_lwu_trivial_count;
INSERT INTO t_lwu_trivial_count SELECT number, number FROM numbers(1000);
UPDATE t_lwu_trivial_count SET v = v + 1 WHERE 1;

SELECT countIf(explain LIKE '%Optimized trivial count%')
FROM (EXPLAIN SELECT count() FROM t_lwu_trivial_count);

SELECT '--- V1: pending delete patches ---';

-- A legacy delete patch must reduce the count to 990 and disable the optimization.
DELETE FROM t_lwu_trivial_count WHERE id < 10
SETTINGS lightweight_delete_mode = 'lightweight_update_force';

SELECT count() FROM t_lwu_trivial_count;

SELECT countIf(explain LIKE '%Optimized trivial count%')
FROM (EXPLAIN SELECT count() FROM t_lwu_trivial_count);

DROP TABLE t_lwu_trivial_count;

SELECT '--- FINAL: value-only patches ---';

-- `FINAL` must still deduplicate rows when patches only change values.
CREATE TABLE t_lwu_trivial_count
(
    id UInt64,
    v UInt64,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id
SETTINGS enable_block_number_column = 1,
    enable_block_offset_column = 1,
    patch_parts_version = 'v2';

SYSTEM STOP MERGES t_lwu_trivial_count;

-- Insert two parts containing different versions of the same 500 keys.
INSERT INTO t_lwu_trivial_count SELECT number, number, number FROM numbers(500);
INSERT INTO t_lwu_trivial_count SELECT number, number + 500, number + 500 FROM numbers(500);
UPDATE t_lwu_trivial_count SET v = 0 WHERE id < 10;

-- A plain count sees 1000 rows; `FINAL` keeps only the latest version of each key.
SELECT count() FROM t_lwu_trivial_count;

SELECT count() FROM t_lwu_trivial_count FINAL;

DROP TABLE t_lwu_trivial_count;
