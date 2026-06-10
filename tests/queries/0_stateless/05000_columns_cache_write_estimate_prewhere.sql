-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- The columns cache write estimate budget
-- (columns_cache_max_estimated_compressed_bytes_to_write_to_cache) must account
-- for all columns the query reads, including PREWHERE columns, not only the
-- result columns.

SET max_threads = 1;

DROP TABLE IF EXISTS t_cc_estimate;

CREATE TABLE t_cc_estimate (k UInt64, payload String)
ENGINE = MergeTree ORDER BY k
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192;

-- Random strings barely compress: `payload` is ~10 MB compressed, while the
-- monotonic `k` compresses to well under 1 MB.
INSERT INTO t_cc_estimate SELECT number, randomPrintableASCII(500) FROM numbers(20000);

SYSTEM DROP COLUMNS CACHE;

-- The estimate budget is far below the compressed size of `payload` (read by
-- the PREWHERE step) but far above the compressed size of `k` (the only result
-- column). The estimate must count the PREWHERE column, so the budget is
-- exceeded and nothing may be written to the cache.
SELECT sum(k) > 0 FROM t_cc_estimate PREWHERE length(payload) > 0
SETTINGS use_columns_cache = 1, columns_cache_max_estimated_compressed_bytes_to_write_to_cache = 1000000;

SELECT count() FROM system.columns_cache;

-- With a budget large enough for all read columns, writes happen.
SYSTEM DROP COLUMNS CACHE;

SELECT sum(k) > 0 FROM t_cc_estimate PREWHERE length(payload) > 0
SETTINGS use_columns_cache = 1, columns_cache_max_estimated_compressed_bytes_to_write_to_cache = 1000000000;

SELECT count() > 0 FROM system.columns_cache;

DROP TABLE t_cc_estimate;
