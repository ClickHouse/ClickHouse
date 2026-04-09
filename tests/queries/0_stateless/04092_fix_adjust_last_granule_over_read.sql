-- Tags: no-random-merge-tree-settings
-- Regression test for MergeTreeReaderWide::readRows over-read clamping.
--
-- Bug: readRows() could return more rows than max_rows_to_read when
-- deserialization (SubstreamsCache interactions for Map/Nested subcolumns)
-- grew a column beyond the requested limit. This caused
-- MergeTreeRangeReader::adjustLastGranule() to hit an unsigned underflow
-- (num_read_rows > total_rows_per_granule), throwing LOGICAL_ERROR
-- "Can't adjust last granule".
--
-- The bug manifested under MSan/TSan sanitizer builds in stress tests
-- with FINAL queries reading Map subcolumns from Wide MergeTree parts.
-- This test exercises the same code path: ReplacingMergeTree FINAL with
-- Map subcolumns, small index_granularity, multiple overlapping parts,
-- and various max_block_size values that force partial granule reads
-- and range transitions in startReadingChain.
--
-- See: https://github.com/ClickHouse/ClickHouse/issues/100769

DROP TABLE IF EXISTS t_readrows_clamp;

CREATE TABLE t_readrows_clamp
(
    key UInt64,
    ver UInt64,
    m Map(String, UInt64)
)
ENGINE = ReplacingMergeTree(ver)
ORDER BY key
SETTINGS index_granularity = 64, min_bytes_for_wide_part = 0;

-- Create multiple overlapping parts to exercise FINAL deduplication.
-- Overlapping keys force the FINAL merge to read with can_read_incomplete_granules=true,
-- and multiple parts create multiple mark ranges per read task.
INSERT INTO t_readrows_clamp SELECT
    number % 500, number,
    map('a', number, 'b', number + 1, 'c', number + 2)
FROM numbers(2000);

INSERT INTO t_readrows_clamp SELECT
    number % 500, number + 2000,
    map('a', number * 2, 'd', number)
FROM numbers(2000);

INSERT INTO t_readrows_clamp SELECT
    number % 500, number + 4000,
    map('b', number * 3, 'e', number, 'f', number + 1)
FROM numbers(2000);

-- FINAL queries with various max_block_size values.
-- Small max_block_size forces partial granule reads and range transitions
-- in startReadingChain, exercising the code path where adjustLastGranule
-- is called after a carried-over stream is finalized.
SELECT count() FROM t_readrows_clamp FINAL;
SELECT count() FROM t_readrows_clamp FINAL SETTINGS max_block_size = 100;
SELECT count() FROM t_readrows_clamp FINAL SETTINGS max_block_size = 37;
SELECT count() FROM t_readrows_clamp FINAL SETTINGS max_block_size = 7;

-- Map subcolumn reads that exercise SubstreamsCache sharing between
-- subcolumns of the same Map column within a single readRows call.
SELECT count() FROM (SELECT m.size0, m.key_a FROM t_readrows_clamp FINAL);
SELECT count() FROM (SELECT m.size0, m.key_a FROM t_readrows_clamp FINAL SETTINGS max_block_size = 50);
SELECT count() FROM (SELECT m.key_a, m.key_b, m.key_c FROM t_readrows_clamp FINAL SETTINGS max_block_size = 33);
SELECT count() FROM (SELECT m, m.keys, m.values, m.size0, m.key_a FROM t_readrows_clamp FINAL SETTINGS max_block_size = 17);

-- Verify data correctness.
SELECT sum(key) FROM t_readrows_clamp FINAL;
SELECT sum(key) FROM t_readrows_clamp FINAL SETTINGS max_block_size = 37;

DROP TABLE t_readrows_clamp;
