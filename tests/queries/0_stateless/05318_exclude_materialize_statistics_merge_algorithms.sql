-- exclude_materialize_statistics_on_merge with the vertical merge algorithm, and with a merge that can remove rows
-- (ReplacingMergeTree), which builds the statistics of the new part from the merged rows instead of merging the
-- statistics of the merged parts.
SET allow_statistics = 1;
SET materialize_statistics_on_insert = 0;

DROP TABLE IF EXISTS tab_vertical;
DROP TABLE IF EXISTS tab_replacing;

-- The vertical algorithm merges the sorting key column `a` in its first stage and the other columns one by one in
-- its second stage: exclude a column of each stage.
CREATE TABLE tab_vertical
(
    a UInt64,
    b UInt64,
    c UInt64
)
ENGINE = MergeTree
ORDER BY a
SETTINGS
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = 'basic',
    exclude_materialize_statistics_on_merge = 'a, b',
    min_bytes_for_wide_part = 0,
    min_rows_for_wide_part = 0,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;

SYSTEM STOP MERGES tab_vertical;
INSERT INTO tab_vertical SELECT number, number, number FROM numbers(100);
INSERT INTO tab_vertical SELECT number + 100, number, number FROM numbers(100);
SYSTEM START MERGES tab_vertical;
OPTIMIZE TABLE tab_vertical FINAL;

SELECT 'Vertical merge excludes a and b';
SELECT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab_vertical' AND active
ORDER BY column;

CREATE TABLE tab_replacing
(
    a UInt64,
    b UInt64,
    c UInt64
)
ENGINE = ReplacingMergeTree
ORDER BY a
SETTINGS
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    auto_statistics_types = 'basic',
    exclude_materialize_statistics_on_merge = 'b';

SYSTEM STOP MERGES tab_replacing;
INSERT INTO tab_replacing SELECT number, number, number FROM numbers(100);
-- Replaces 50 of the rows.
INSERT INTO tab_replacing SELECT number, number + 1, number + 1 FROM numbers(50);
SYSTEM START MERGES tab_replacing;
OPTIMIZE TABLE tab_replacing FINAL;

SELECT 'ReplacingMergeTree merge removes rows and excludes b';
SELECT rows FROM system.parts WHERE database = currentDatabase() AND table = 'tab_replacing' AND active;
SELECT column, statistics != [] AS has_stats
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'tab_replacing' AND active
ORDER BY column;

DROP TABLE tab_vertical;
DROP TABLE tab_replacing;
