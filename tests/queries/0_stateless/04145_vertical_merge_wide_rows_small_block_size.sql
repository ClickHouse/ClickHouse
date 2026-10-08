-- Tags: no-random-merge-tree-settings, no-random-settings
-- Test: regression test for endless loop in vertical merge with small `merge_max_block_size_bytes`
--       and wide rows. Without the fix from PR #59812, OPTIMIZE FINAL would never terminate.
-- Covers:
--   src/Processors/Transforms/ColumnGathererTransform.h:148-198 — `do { ... } while` loop
--     guaranteeing forward progress when `column_res.byteSize()` is already past
--     `block_preferred_size_bytes` on entry, plus the switch from `allocatedBytes()` to `byteSize()`.
--   src/Processors/Merges/Algorithms/MergedData.h:103 — `chunk.bytes()` (was `allocatedBytes`).
--   src/Processors/Merges/Algorithms/MergedData.h:125 — `column->byteSize()` (was `allocatedBytes`).

DROP TABLE IF EXISTS t_vmerge_wide;

CREATE TABLE t_vmerge_wide
(
    id UInt64,
    s String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    index_granularity = 8192,
    index_granularity_bytes = 0,
    enable_mixed_granularity_parts = 0,
    merge_max_block_size = 8192,
    merge_max_block_size_bytes = 1024;

-- Insert with interleaved IDs across three parts so vertical merge alternates
-- between sources rather than consuming a full source as one block (this prevents
-- the source_to_fully_copy fast path and forces the gather body to iterate).
-- Each row's String is ~10000 bytes >> merge_max_block_size_bytes (1024), so on
-- subsequent gather() calls the carried-over column_res.byteSize() is already past
-- the threshold — exercising the do/while fix that ensures at least one iteration.
INSERT INTO t_vmerge_wide SELECT number * 3, repeat('x', 10000) FROM numbers(50);
INSERT INTO t_vmerge_wide SELECT number * 3 + 1, repeat('y', 10000) FROM numbers(50);
INSERT INTO t_vmerge_wide SELECT number * 3 + 2, repeat('z', 10000) FROM numbers(50);

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_vmerge_wide' AND active;

OPTIMIZE TABLE t_vmerge_wide FINAL;

-- All 150 rows must be present after the merge. Without the fix, OPTIMIZE FINAL would never finish.
SELECT count(), sum(length(s)) FROM t_vmerge_wide;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_vmerge_wide' AND active;

-- Spot checks — verify rows from each input part survived with correct data.
SELECT id, length(s), substring(s, 1, 1) FROM t_vmerge_wide WHERE id IN (0, 1, 2, 147, 148, 149) ORDER BY id;

DROP TABLE t_vmerge_wide;
