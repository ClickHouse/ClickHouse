-- Regression test: with marks_compression_codec = 'NONE', mark blocks must honor
-- marks_compress_block_size instead of being clamped to the small (4096 bytes) file buffer
-- of the marks file. If they were clamped, a table with a large marks_compress_block_size
-- would produce the same frames (and thus the same marks file size) as a table with a small
-- one, and the size comparisons below would return 0.

DROP TABLE IF EXISTS marks_none_wide_big;
DROP TABLE IF EXISTS marks_none_wide_small;
DROP TABLE IF EXISTS marks_none_compact_big;
DROP TABLE IF EXISTS marks_none_compact_small;

CREATE TABLE marks_none_wide_big (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS compress_marks = 1, marks_compression_codec = 'NONE', marks_compress_block_size = 65536,
             index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE marks_none_wide_small (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS compress_marks = 1, marks_compression_codec = 'NONE', marks_compress_block_size = 4096,
             index_granularity = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

CREATE TABLE marks_none_compact_big (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS compress_marks = 1, marks_compression_codec = 'NONE', marks_compress_block_size = 65536,
             index_granularity = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

CREATE TABLE marks_none_compact_small (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS compress_marks = 1, marks_compression_codec = 'NONE', marks_compress_block_size = 4096,
             index_granularity = 1, min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

-- 2000 granules (index_granularity = 1) already produce ~48 KB of marks per column,
-- which spans a dozen 4096-byte frames but only one or two 65536-byte frames, so the size
-- comparison below is unambiguous. A larger row count is not needed and only makes the test
-- run slow enough to trip the "Test runs too long" flaky-check limit under sanitizers.
INSERT INTO marks_none_wide_big SELECT number FROM numbers(2000);
INSERT INTO marks_none_wide_small SELECT number FROM numbers(2000);
INSERT INTO marks_none_compact_big SELECT number FROM numbers(2000);
INSERT INTO marks_none_compact_small SELECT number FROM numbers(2000);

-- Sanity: the pairs actually exercise both the wide and the compact marks writer.
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table LIKE 'marks_none_wide_%' AND active;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table LIKE 'marks_none_compact_%' AND active;

-- Every frame of NONE-coded marks carries a fixed number of service bytes (checksum + header),
-- so the file written with the configured 65536-byte blocks must be strictly smaller than the
-- file written with 4096-byte blocks. If the block size were clamped to the marks file buffer,
-- both files would have identical framing and equal sizes.
SELECT (SELECT sum(marks_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'marks_none_wide_big' AND active)
     < (SELECT sum(marks_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'marks_none_wide_small' AND active);

SELECT (SELECT sum(marks_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'marks_none_compact_big' AND active)
     < (SELECT sum(marks_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'marks_none_compact_small' AND active);

-- The marks must still be readable: a full scan and a primary-key range read.
SELECT count(), sum(n) FROM marks_none_wide_big;
SELECT count(), sum(n) FROM marks_none_wide_big WHERE n >= 1990;
SELECT count(), sum(n) FROM marks_none_compact_big;
SELECT count(), sum(n) FROM marks_none_compact_big WHERE n >= 1990;

DROP TABLE marks_none_wide_big;
DROP TABLE marks_none_wide_small;
DROP TABLE marks_none_compact_big;
DROP TABLE marks_none_compact_small;
