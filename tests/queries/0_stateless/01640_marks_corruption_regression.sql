-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS adaptive_table;

CREATE TABLE adaptive_table(
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY key
SETTINGS
    index_granularity_bytes=1048576,
    min_bytes_for_wide_part=0,
    old_parts_lifetime=0,
    index_granularity=8192
;

-- This triggers adjustment of the granules that was introduced in PR#17120
INSERT INTO adaptive_table SELECT number, randomPrintableASCII(if(number BETWEEN 8192-30 AND 8192, 102400, 1)) FROM system.numbers LIMIT 16384;
-- This creates the following marks:
--
--     $ check-marks /path/to/db/adaptive_table/all_*/key.{mrk2,bin}
--     Mark 0, points to 0, 0, has rows after 8192, decompressed size 72808. <!-- wrong number of rows, should be 5461
--     Mark 1, points to 0, 43688, has rows after 1820, decompressed size 29120.
--     Mark 2, points to 0, 58248, has rows after 1820, decompressed size 14560.
--     Mark 3, points to 36441, 0, has rows after 1820, decompressed size 58264.
--     Mark 4, points to 36441, 14560, has rows after 1820, decompressed size 43704.
--     Mark 5, points to 36441, 29120, has rows after 8192, decompressed size 29144.
--     Mark 6, points to 36441, 58264, has rows after 0, decompressed size 0.
OPTIMIZE TABLE adaptive_table FINAL;

SELECT 'marks', marks FROM system.parts WHERE table = 'adaptive_table' AND database = currentDatabase() AND active FORMAT CSV;

-- Reset marks cache
DETACH TABLE adaptive_table;
ATTACH TABLE adaptive_table;

-- This works correctly, since it does not read any marks
SELECT 'optimize_trivial_count_query', count() FROM adaptive_table SETTINGS
    optimize_trivial_count_query=1
FORMAT CSV;
-- This works correctly, since it reads marks sequentially and don't seek
SELECT 'max_threads=1', count() FROM adaptive_table SETTINGS
    optimize_trivial_count_query=0,
    max_threads=1
FORMAT CSV;
-- This works wrong, since it seek to each mark (due to reading each mark from a separate thread),
-- so if the marks offsets will be wrong it will read more data.
--
-- Reading each mark from a separate thread is just the simplest reproducers,
-- this can be also reproduced with PREWHERE since it skips data physically,
-- so it also uses seeks.
SELECT 'max_threads=100', count() FROM adaptive_table SETTINGS
    optimize_trivial_count_query=0,
    merge_tree_min_rows_for_concurrent_read=1,
    merge_tree_min_bytes_for_concurrent_read=1,
    max_threads=100
FORMAT CSV;

DROP TABLE adaptive_table;
