-- Tags: no-random-merge-tree-settings
-- Regression test for STID 0883: extreme max_compress_block_size should not crash the server.
-- The value is silently clamped to 256 MiB to prevent absurd memory allocations.

DROP TABLE IF EXISTS t_extreme_compress;

-- Test 1: Wide parts with extreme max_compress_block_size (STID 0883-4864 variant)
CREATE TABLE t_extreme_compress (x UInt64, s String)
    ENGINE = MergeTree() ORDER BY x
    SETTINGS max_compress_block_size = 9223372036854775807;
INSERT INTO t_extreme_compress SELECT number, toString(number) FROM numbers(1000);
SELECT count() FROM t_extreme_compress;
DROP TABLE t_extreme_compress;

-- Test 2: Compact parts with extreme max_compress_block_size (STID 0883-5a5b path via compact writer)
CREATE TABLE t_extreme_compress (x UInt64)
    ENGINE = MergeTree() ORDER BY x
    SETTINGS max_compress_block_size = 9223372036854775807, min_bytes_for_wide_part = 999999999999;
INSERT INTO t_extreme_compress SELECT number FROM numbers(1000);
SELECT count() FROM t_extreme_compress;
DROP TABLE t_extreme_compress;

-- Test 3: Extreme primary_key_compress_block_size (initPrimaryIndex path)
CREATE TABLE t_extreme_compress (x UInt64)
    ENGINE = MergeTree() ORDER BY x
    SETTINGS compress_primary_key = 1, primary_key_compress_block_size = 9223372036854775807;
INSERT INTO t_extreme_compress SELECT number FROM numbers(1000);
SELECT count() FROM t_extreme_compress;
DROP TABLE t_extreme_compress;

-- Test 4: Extreme marks_compress_block_size
CREATE TABLE t_extreme_compress (x UInt64)
    ENGINE = MergeTree() ORDER BY x
    SETTINGS marks_compress_block_size = 9223372036854775807;
INSERT INTO t_extreme_compress SELECT number FROM numbers(1000);
SELECT count() FROM t_extreme_compress;
DROP TABLE t_extreme_compress;

-- Test 5: Extreme min_compress_block_size
CREATE TABLE t_extreme_compress (x UInt64)
    ENGINE = MergeTree() ORDER BY x
    SETTINGS min_compress_block_size = 9223372036854775807;
INSERT INTO t_extreme_compress SELECT number FROM numbers(1000);
SELECT count() FROM t_extreme_compress;
DROP TABLE t_extreme_compress;

-- Test 6: Column-level max_compress_block_size override on wide parts
-- Covers the clamp in MergeTreeDataPartWriterWide::addStreams
CREATE TABLE t_extreme_compress (x UInt64, s String SETTINGS (max_compress_block_size = 9223372036854775807))
    ENGINE = MergeTree() ORDER BY x
    SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_extreme_compress SELECT number, toString(number) FROM numbers(1000);
SELECT count() FROM t_extreme_compress;
DROP TABLE t_extreme_compress;
