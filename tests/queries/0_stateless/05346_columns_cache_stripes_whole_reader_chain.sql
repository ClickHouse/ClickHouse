-- Tags: no-random-settings, no-parallel-replicas
-- Random settings (splitting of ranges, direct IO) change how a read is split into blocks.
-- Parallel replicas hand out mark ranges that are not aligned with the stripes of the columns cache.

-- The blocks of a read are aligned with the stripes of the columns cache (65536 rows here) by every reader of the
-- chain together: a skip index evaluated at read time does not turn the alignment off, and a whole stripe is not read
-- for a cached PREWHERE column when the rest of the row is read from disk.

SET use_columns_cache = 1, max_threads = 1, max_block_size = 65409, preferred_block_size_bytes = 1000000;

DROP TABLE IF EXISTS t_skip_index;
CREATE TABLE t_skip_index (k UInt64, v UInt64, INDEX ix v TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_skip_index SELECT number, number FROM numbers(1048576);

-- The range starts in the middle of a stripe, so the first block ends at the end of the stripe.
SELECT 'skip index evaluated before reading', groupArray(b) FROM
    (SELECT b FROM (SELECT blockSize() AS b, rowNumberInBlock() AS r, k FROM t_skip_index WHERE k >= 24576 AND v >= 24576 AND v < 1000000) WHERE r = 0)
SETTINGS use_skip_indexes_on_data_read = 0;

SELECT 'skip index evaluated on data read', groupArray(b) FROM
    (SELECT b FROM (SELECT blockSize() AS b, rowNumberInBlock() AS r, k FROM t_skip_index WHERE k >= 24576 AND v >= 24576 AND v < 1000000) WHERE r = 0)
SETTINGS use_skip_indexes_on_data_read = 1;

DROP TABLE t_skip_index;

DROP TABLE IF EXISTS t_prewhere;
CREATE TABLE t_prewhere (k UInt64, s String) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
INSERT INTO t_prewhere SELECT number, repeat('x', 1000) FROM numbers(262144);

-- `k` is cached, `s` is not and is not written to the cache: a block of `s` keeps to `preferred_block_size_bytes`,
-- up to the end of the stripe at twice the size.
SELECT count() FROM t_prewhere WHERE NOT ignore(k) FORMAT Null;
SELECT 'prewhere', max(blockSize()) <= 16384 FROM t_prewhere PREWHERE NOT ignore(k) WHERE NOT ignore(s)
SETTINGS enable_writes_to_columns_cache = 0;

DROP TABLE t_prewhere;
