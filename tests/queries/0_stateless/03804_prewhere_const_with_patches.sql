-- Tags: no-replicated-database
-- Test for constant PREWHERE with patch parts (lightweight updates)
-- This tests the fix for the exception "Can't adjust last granule" when using
-- constant PREWHERE expressions with tables that have patch parts.

DROP TABLE IF EXISTS t_prewhere_const_patches SYNC;

SET enable_lightweight_update = 1;
SET mutations_sync = 2;

CREATE TABLE t_prewhere_const_patches (a UInt64, b UInt64, c UInt64, d UInt64)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    ratio_of_defaults_for_sparse_serialization = 1.0,
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

-- Stop merges to ensure patch parts are preserved
SYSTEM STOP MERGES t_prewhere_const_patches;

INSERT INTO t_prewhere_const_patches SELECT number, 0, 0, 0 FROM numbers(10000);

-- Apply some lightweight updates to create patch parts
UPDATE t_prewhere_const_patches SET b = 1 WHERE a % 4 = 0;
UPDATE t_prewhere_const_patches SET c = 2 WHERE a % 4 = 0;

-- This query with constant PREWHERE (18 is a non-zero integer, treated as true)
-- used to cause "Can't adjust last granule" exception.
-- Just verify it doesn't crash and returns the correct count.
SELECT count() FROM t_prewhere_const_patches PREWHERE 18;

-- Also test with explicit true constant
SELECT count() FROM t_prewhere_const_patches PREWHERE 1;

-- Test with false constant (should return 0)
SELECT count() FROM t_prewhere_const_patches PREWHERE 0;

-- Test the original fuzzed query pattern with simpler output
SELECT b, c, count() FROM t_prewhere_const_patches PREWHERE 18 GROUP BY b, c ORDER BY b, c;

DROP TABLE IF EXISTS t_prewhere_const_patches SYNC;
