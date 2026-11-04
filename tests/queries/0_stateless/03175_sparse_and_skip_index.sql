DROP TABLE IF EXISTS t_bloom_filter;
CREATE TABLE t_bloom_filter(
  key UInt64,
  value UInt64,

  INDEX key_bf key TYPE bloom_filter(0.01) GRANULARITY 2147483648, -- bloom filter on sorting key column
  INDEX value_bf value TYPE bloom_filter(0.01) GRANULARITY 2147483648  -- bloom filter on no-sorting column
) ENGINE=MergeTree ORDER BY key
SETTINGS
    -- settings to trigger sparse serialization and vertical merge
    ratio_of_defaults_for_sparse_serialization = 0.0,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    allow_vertical_merges_from_compact_to_wide_parts = 1,
    min_bytes_for_wide_part = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0;

SYSTEM STOP MERGES t_bloom_filter;

-- Create at least one part
INSERT INTO t_bloom_filter
SELECT
    number % 100 as key, -- 100 unique keys
    rand() % 100 as value -- 100 unique values
FROM numbers(15_000);

-- And another part
INSERT INTO t_bloom_filter
SELECT
    number % 100 as key, -- 100 unique keys
    rand() % 100 as value -- 100 unique values
FROM numbers(15_000, 15_000);

SYSTEM START MERGES t_bloom_filter;

-- Merge everything into a single part
OPTIMIZE TABLE t_bloom_filter FINAL;

-- Check sparse serialization
SELECT column, serialization_kind FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_bloom_filter' AND active ORDER BY column;

SELECT COUNT() FROM t_bloom_filter WHERE key = 1;

-- Check bloom filter non-zero size
SELECT COUNT() FROM system.parts WHERE database = currentDatabase() AND table = 't_bloom_filter' AND secondary_indices_uncompressed_bytes > 200 AND active;
