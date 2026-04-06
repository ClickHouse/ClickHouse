-- Tags: no-random-merge-tree-settings
-- Regression test for MergeTreeReaderWide::readRows over-read clamping.
--
-- Bug: readRows() could return more rows than max_rows_to_read when
-- deserialization (SubstreamsCache interactions in composite columns like
-- Map/Nested) grew a column beyond the requested limit. This caused
-- MergeTreeRangeReader::adjustLastGranule() to hit an unsigned underflow
-- (num_read_rows > total_rows_per_granule), throwing LOGICAL_ERROR
-- "Can't adjust last granule".
--
-- The bug only manifested in FINAL mode (can_read_incomplete_granules=true)
-- with specific timing under TSAN/MSAN stress. This test exercises the
-- same code path: ReplacingMergeTree FINAL with Map/Nested columns, small
-- index_granularity, multiple parts, and various max_block_size values
-- that force partial granule reads.
--
-- See: https://github.com/ClickHouse/ClickHouse/issues/100769

DROP TABLE IF EXISTS t_wide_readrows_clamp;

CREATE TABLE t_wide_readrows_clamp
(
    key UInt64,
    ver UInt64,
    payload String,
    m Map(String, UInt64),
    n Nested(a UInt64, b String)
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY key
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0;

-- Create multiple parts with overlapping keys to exercise FINAL deduplication.
-- Each insert creates a separate part. Overlapping keys force the FINAL
-- merge to read with can_read_incomplete_granules=true.
INSERT INTO t_wide_readrows_clamp SELECT
    number % 500, number,
    repeat('x', number % 50),
    map('k' || toString(number % 10), number),
    [number % 3, number % 5], ['aa', 'bb']
FROM numbers(1000);

INSERT INTO t_wide_readrows_clamp SELECT
    number % 500, number + 1000,
    repeat('y', number % 30),
    map('k' || toString(number % 7), number * 2, 'extra', number),
    [number % 4, number % 6, number % 8], ['cc', 'dd', 'ee']
FROM numbers(1000);

INSERT INTO t_wide_readrows_clamp SELECT
    number % 500, number + 2000,
    repeat('z', number % 40),
    map('k' || toString(number % 13), number * 3),
    [number % 2], ['ff']
FROM numbers(1000);

-- FINAL queries with various max_block_size values.
-- Small max_block_size forces partial granule reads in FINAL mode,
-- exercising the code path where adjustLastGranule is called.
-- Without the readRows clamp fix, these could crash with LOGICAL_ERROR
-- under TSAN/MSAN when deserialization over-reads.
SELECT count() FROM t_wide_readrows_clamp FINAL;
SELECT count() FROM t_wide_readrows_clamp FINAL SETTINGS max_block_size = 100;
SELECT count() FROM t_wide_readrows_clamp FINAL SETTINGS max_block_size = 37;
SELECT count() FROM t_wide_readrows_clamp FINAL SETTINGS max_block_size = 7;

-- Also verify data correctness: sum of keys should match
SELECT sum(key) FROM t_wide_readrows_clamp FINAL;
SELECT sum(key) FROM t_wide_readrows_clamp FINAL SETTINGS max_block_size = 37;

DROP TABLE t_wide_readrows_clamp;
