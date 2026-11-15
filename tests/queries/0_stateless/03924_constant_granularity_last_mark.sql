-- Tags: no-random-settings, no-random-merge-tree-settings

-- Regression test for incorrect last mark granularity in constant granularity parts.
-- When index_granularity_bytes = 0 (non-adaptive marks), the last granule may have
-- fewer rows than the declared granularity. The in-memory MergeTreeIndexGranularityConstant
-- must reflect the actual last granule size, otherwise _part_offset produces values
-- beyond the real row count, breaking lazy materialization with ORDER BY ... LIMIT.

DROP TABLE IF EXISTS tab_const;

CREATE TABLE tab_const (id UInt64, v1 UInt64, v2 String)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 64, index_granularity_bytes = 0, min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;

INSERT INTO tab_const SELECT number, number, toString(number) FROM numbers(10000);

-- Verify _part_offset is bounded by the actual row count.
SELECT count(), max(_part_offset) FROM tab_const;

-- The bug manifested when max_block_size equals the actual last granule size (16 rows),
-- causing ghost rows and a LOGICAL_ERROR in LazyMaterializingTransform.
SELECT * FROM tab_const ORDER BY v1 LIMIT 2 SETTINGS max_threads = 1, max_block_size = 16;

-- Also test with various block sizes to catch edge cases.
SELECT * FROM tab_const ORDER BY v1 LIMIT 2 SETTINGS max_threads = 1, max_block_size = 10;
SELECT * FROM tab_const ORDER BY v1 LIMIT 2 SETTINGS max_threads = 4, max_block_size = 64;
SELECT * FROM tab_const ORDER BY v1 LIMIT 2 SETTINGS max_threads = 1, max_block_size = 100;

DROP TABLE tab_const;
