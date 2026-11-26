-- Tags: no-random-merge-tree-settings

-- Test that CLEAR COLUMN properly handles all dependent objects:
-- projections, skip indices, materialized columns, and statistics.

SET mutations_sync = 2;

-- ============================================================
-- 1. Projections: clearing a column must rebuild projections
--    that reference it with the type-default value.
-- ============================================================

-- 1a. Compact parts
DROP TABLE IF EXISTS test_proj_compact;
CREATE TABLE test_proj_compact
(
    c0 Int32,
    c1 Int32,
    PROJECTION p0 (SELECT * ORDER BY c1)
)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = '100G';

INSERT INTO test_proj_compact SELECT number, number FROM numbers(10);
ALTER TABLE test_proj_compact CLEAR COLUMN c1;

-- After clearing, c1 = 0 everywhere, projection is rebuilt.
SELECT 'proj_compact', count() FROM test_proj_compact WHERE c1 = 0 SETTINGS optimize_use_projections = 1;
SELECT 'proj_compact', count() FROM test_proj_compact WHERE c1 = 0 SETTINGS optimize_use_projections = 0;
DROP TABLE test_proj_compact;

-- 1b. Wide parts
DROP TABLE IF EXISTS test_proj_wide;
CREATE TABLE test_proj_wide
(
    c0 Int32,
    c1 Int32,
    PROJECTION p0 (SELECT * ORDER BY c1)
)
ENGINE = MergeTree() ORDER BY tuple()
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1;

INSERT INTO test_proj_wide SELECT number, number FROM numbers(10);
ALTER TABLE test_proj_wide CLEAR COLUMN c1;

SELECT 'proj_wide', count() FROM test_proj_wide WHERE c1 = 0 SETTINGS optimize_use_projections = 1;
SELECT 'proj_wide', count() FROM test_proj_wide WHERE c1 = 0 SETTINGS optimize_use_projections = 0;
DROP TABLE test_proj_wide;

-- ============================================================
-- 2. Skip indices: clearing a column must drop skip indices
--    that depend on it so stale index data does not cause
--    incorrect filtering.
-- ============================================================

-- 2a. Compact parts
DROP TABLE IF EXISTS test_idx_compact;
CREATE TABLE test_idx_compact
(
    key Int32,
    val Int32,
    INDEX idx_val val TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree() ORDER BY key
SETTINGS index_granularity = 1, min_bytes_for_wide_part = '100G';

INSERT INTO test_idx_compact SELECT number, number + 1 FROM numbers(10);
-- val has values 1..10.  After clear, val = 0.
ALTER TABLE test_idx_compact CLEAR COLUMN val;

-- Old minmax index would say "val in [1, 10]" -> skip granule for val = 0.
-- After fix, the index is dropped, so the query must find all 10 rows.
SELECT 'idx_compact', count() FROM test_idx_compact WHERE val = 0;
DROP TABLE test_idx_compact;

-- 2b. Wide parts
DROP TABLE IF EXISTS test_idx_wide;
CREATE TABLE test_idx_wide
(
    key Int32,
    val Int32,
    INDEX idx_val val TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree() ORDER BY key
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1;

INSERT INTO test_idx_wide SELECT number, number + 1 FROM numbers(10);
ALTER TABLE test_idx_wide CLEAR COLUMN val;

SELECT 'idx_wide', count() FROM test_idx_wide WHERE val = 0;
DROP TABLE test_idx_wide;

-- ============================================================
-- 3. Materialized columns: clearing a source column must
--    recalculate MATERIALIZED columns that depend on it.
-- ============================================================

-- 3a. Compact parts
DROP TABLE IF EXISTS test_mat_compact;
CREATE TABLE test_mat_compact
(
    c0 Int32,
    c1 Int32,
    c2 Int32 MATERIALIZED c1 + 10
)
ENGINE = MergeTree() ORDER BY c0
SETTINGS index_granularity = 1, min_bytes_for_wide_part = '100G';

INSERT INTO test_mat_compact (c0, c1) SELECT number, number FROM numbers(5);
-- c2 was materialized as c1 + 10, so c2 has values 10..14.
SELECT 'mat_compact_before', sum(c2) FROM test_mat_compact;

ALTER TABLE test_mat_compact CLEAR COLUMN c1;
-- After clearing c1, c1 = 0, so c2 should be recalculated to 0 + 10 = 10.
SELECT 'mat_compact_after', sum(c2) FROM test_mat_compact;
DROP TABLE test_mat_compact;

-- 3b. Wide parts
DROP TABLE IF EXISTS test_mat_wide;
CREATE TABLE test_mat_wide
(
    c0 Int32,
    c1 Int32,
    c2 Int32 MATERIALIZED c1 + 10
)
ENGINE = MergeTree() ORDER BY c0
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1;

INSERT INTO test_mat_wide (c0, c1) SELECT number, number FROM numbers(5);
SELECT 'mat_wide_before', sum(c2) FROM test_mat_wide;

ALTER TABLE test_mat_wide CLEAR COLUMN c1;
SELECT 'mat_wide_after', sum(c2) FROM test_mat_wide;
DROP TABLE test_mat_wide;

-- ============================================================
-- 4. Combined: projection + materialized column depend on the
--    same cleared column.
-- ============================================================

DROP TABLE IF EXISTS test_combined;
CREATE TABLE test_combined
(
    c0 Int32,
    c1 Int32,
    c2 Int32 MATERIALIZED c1 * 2,
    PROJECTION p0 (SELECT * ORDER BY c1)
)
ENGINE = MergeTree() ORDER BY c0
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1;

INSERT INTO test_combined (c0, c1) SELECT number, number FROM numbers(5);
ALTER TABLE test_combined CLEAR COLUMN c1;

-- c1 = 0, c2 should be 0 * 2 = 0, projection rebuilt.
SELECT 'combined', count() FROM test_combined WHERE c1 = 0 SETTINGS optimize_use_projections = 1;
SELECT 'combined', sum(c2) FROM test_combined;
DROP TABLE test_combined;

-- ============================================================
-- 5. Multi-column clear: a single ALTER clears two columns that
--    both feed the same MATERIALIZED expression.  Both must get
--    default values in the readonly stage.
-- ============================================================

DROP TABLE IF EXISTS test_multi_clear;
CREATE TABLE test_multi_clear
(
    c0 Int32,
    c1 Int32,
    c2 Int32,
    m Int32 MATERIALIZED c1 + c2
)
ENGINE = MergeTree() ORDER BY c0
SETTINGS index_granularity = 1, min_bytes_for_wide_part = 1;

INSERT INTO test_multi_clear (c0, c1, c2) SELECT number, number, number * 10 FROM numbers(5);
-- m = c1 + c2 = 0+0, 1+10, 2+20, 3+30, 4+40 => sum = 0+11+22+33+44 = 110
SELECT 'multi_clear_before', sum(m) FROM test_multi_clear;

ALTER TABLE test_multi_clear CLEAR COLUMN c1, CLEAR COLUMN c2;
-- After clearing both: c1 = 0, c2 = 0, m should be recalculated to 0 + 0 = 0.
SELECT 'multi_clear_after', sum(m) FROM test_multi_clear;
DROP TABLE test_multi_clear;
