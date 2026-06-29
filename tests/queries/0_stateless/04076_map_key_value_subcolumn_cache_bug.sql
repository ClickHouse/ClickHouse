-- Test for a bug in SerializationMapKeyValue::deserializeBinaryBulkWithMultipleStreams
-- when reading_full_map = false (only subcolumns, not the full Map), data_part_type = Wide,
-- and both m.size0 and m.key_a subcolumns are read together.
--
-- The bug: m.size0 caches offsets at key "size0" in the substreams cache. When m.key_a's
-- Array serialization reads offsets, it finds this cached column. On the second readRows()
-- call (when result columns already have data from a previous call), the cached offsets
-- column contains ALL accumulated rows, not just the current range. Without the fix
-- (insert_only_rows_in_current_range_from_substreams_cache = true for Wide parts),
-- the Array uses all accumulated offsets and tries to read too many elements.
--
-- To trigger this on local storage, we need non-contiguous mark ranges so that the
-- MergeTreeRangeReader creates multiple Streams, causing multiple readRows() calls
-- that accumulate into the same result columns. We achieve this with a WHERE on the
-- primary key that creates a gap in the selected mark ranges.

DROP TABLE IF EXISTS t_map_cache_bug;

CREATE TABLE t_map_cache_bug (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'basic',
    map_serialization_version_for_zero_level_parts = 'basic',
    min_bytes_for_wide_part = 1,
    min_rows_for_wide_part = 1,
    serialization_info_version = 'with_types',
    index_granularity = 700;

INSERT INTO t_map_cache_bug SELECT number, multiIf(
    number < 9000, map('a', number, 'b', number + 1, 'c', number + 2),
    number < 18000, map('a', number, 'b', number + 1),
    number < 27000, map('c', number, 'd', number + 1),
    number < 36000, map('a', number),
    number < 42000, map('a', number, 'b', number + 1, 'c', number + 2, 'd', number + 3, 'e', number + 4),
    map('b', number)
) FROM numbers(45000);

-- WHERE creates non-contiguous mark ranges: [0, 5) and [10, 65) with a gap at [5, 10).
-- This causes two readRows() calls for the same result columns.
-- max_threads = 1 ensures a single thread processes all ranges in one block.
SELECT m.size0, m.key_a FROM t_map_cache_bug WHERE id < 3500 OR id >= 7000 FORMAT Null SETTINGS max_threads = 1;

-- Also test the full table scan (single contiguous range, should always work).
SELECT m.size0, m.key_a FROM t_map_cache_bug FORMAT Null SETTINGS max_threads = 1;

DROP TABLE t_map_cache_bug;
