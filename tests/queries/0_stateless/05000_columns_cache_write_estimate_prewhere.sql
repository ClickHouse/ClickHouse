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

-- Random strings barely compress: `payload` is ~600 KB compressed, while the
-- monotonic `k` compresses to ~40 KB.
INSERT INTO t_cc_estimate SELECT number, randomPrintableASCII(60) FROM numbers(10000);

SYSTEM DROP COLUMNS CACHE;

-- The estimate budget is far below the compressed size of `payload` (read by
-- the PREWHERE step) but far above the compressed size of `k` (the only result
-- column). The estimate must count the PREWHERE column, so the budget is
-- exceeded and nothing may be written to the cache.
-- optimize_functions_to_subcolumns is disabled so that the PREWHERE step reads
-- the whole `payload` column rather than the small `payload.size0` subcolumn.
SELECT sum(k) > 0 FROM t_cc_estimate PREWHERE length(payload) > 0
SETTINGS use_columns_cache = 1, optimize_functions_to_subcolumns = 0,
    columns_cache_max_estimated_compressed_bytes_to_write_to_cache = 150000;

SELECT count() FROM system.columns_cache;

-- Sanity check: with no estimate budget the same query populates the cache,
-- so the empty cache above is the work of the estimate gate.
SYSTEM DROP COLUMNS CACHE;

SELECT sum(k) > 0 FROM t_cc_estimate PREWHERE length(payload) > 0
SETTINGS use_columns_cache = 1, optimize_functions_to_subcolumns = 0;

SELECT count() > 0 FROM system.columns_cache;

-- A query under the same budget that reads only `k` stays within the estimate
-- and is allowed to write to the cache.
SYSTEM DROP COLUMNS CACHE;

SELECT sum(k) > 0 FROM t_cc_estimate
SETTINGS use_columns_cache = 1,
    columns_cache_max_estimated_compressed_bytes_to_write_to_cache = 150000;

SELECT count() > 0 FROM system.columns_cache;

DROP TABLE t_cc_estimate;
