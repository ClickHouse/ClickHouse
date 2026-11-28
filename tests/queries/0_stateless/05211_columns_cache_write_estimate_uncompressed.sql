-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database

-- The columns cache write estimate is made in uncompressed bytes - the amount the cache is charged
-- for when the data lands in it - and the whole query is charged for it before anything is read.
-- So a query whose data cannot stay in the cache writes nothing at all: neither when its compressed
-- size is a small fraction of the uncompressed one, nor when it consists of many read tasks.

SET max_threads = 4;
SET merge_tree_min_rows_for_concurrent_read = 8192;
SET merge_tree_min_bytes_for_concurrent_read = 1;

DROP TABLE IF EXISTS t_cc_estimate_uncompressed;

CREATE TABLE t_cc_estimate_uncompressed (k UInt64, payload String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

-- About 100 bytes per row uncompressed, and next to nothing compressed.
INSERT INTO t_cc_estimate_uncompressed SELECT number, repeat('a', 100) FROM numbers(200000);

SELECT sum(data_compressed_bytes) < 2000000, sum(data_uncompressed_bytes) > 20000000
FROM system.parts WHERE database = currentDatabase() AND table = 't_cc_estimate_uncompressed' AND active;

SYSTEM DROP COLUMNS CACHE;

-- A budget far above the compressed size but below the uncompressed one: nothing is written.
SELECT max(payload) != '' FROM t_cc_estimate_uncompressed
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1,
    columns_cache_max_estimated_bytes_to_write_to_cache = 10000000;

SELECT count() FROM system.columns_cache WHERE database = currentDatabase();

SYSTEM DROP COLUMNS CACHE;

-- A budget above the uncompressed size: the data is cached.
SELECT max(payload) != '' FROM t_cc_estimate_uncompressed
SETTINGS use_columns_cache = 1, enable_writes_to_columns_cache = 1, enable_reads_from_columns_cache = 1,
    columns_cache_max_estimated_bytes_to_write_to_cache = 100000000;

SELECT count() > 0 FROM system.columns_cache WHERE database = currentDatabase();

DROP TABLE t_cc_estimate_uncompressed;
