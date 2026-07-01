-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-replicated-database
-- The columns cache write estimate budget
-- (columns_cache_max_estimated_compressed_bytes_to_write_to_cache) must account
-- for the bytes read from patch parts (lightweight updates), sized against the
-- patch parts themselves, not only the base part. Otherwise a lightweight update
-- with a large updated column that is tiny (or absent) in the base part passes the
-- gate on the base part's size and then caches the much larger patch-part column.

SET max_threads = 1;
SET enable_lightweight_update = 1;

DROP TABLE IF EXISTS t_cc_patch_estimate;

CREATE TABLE t_cc_patch_estimate (id UInt64, payload String)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192,
    enable_block_number_column = 1, enable_block_offset_column = 1;

-- Base part: `payload` is empty, so the base part is small (dominated by `id`,
-- ~0.4 MB compressed for 100000 monotonic rows).
INSERT INTO t_cc_patch_estimate SELECT number, '' FROM numbers(100000);

-- Lightweight update stores a large `payload` (~20 MB, random strings barely
-- compress) in a wide patch part, read and cached separately from the base part.
UPDATE t_cc_patch_estimate SET payload = randomPrintableASCII(200) WHERE 1;

SYSTEM DROP COLUMNS CACHE;

-- The estimate budget (2 MB) is above the base part's size but far below the
-- patch part's `payload` size. Sizing the estimate against the patch part must
-- exceed the budget, so cache writes are disabled and nothing is cached.
SELECT sum(length(payload)) > 0 FROM t_cc_patch_estimate
SETTINGS use_columns_cache = 1,
    columns_cache_max_estimated_compressed_bytes_to_write_to_cache = 2000000;

SELECT count() FROM system.columns_cache WHERE database = currentDatabase();

-- Sanity check: with a large estimate budget the same query populates the cache,
-- so the empty cache above is the work of the estimate gate.
SYSTEM DROP COLUMNS CACHE;

SELECT sum(length(payload)) > 0 FROM t_cc_patch_estimate
SETTINGS use_columns_cache = 1,
    columns_cache_max_estimated_compressed_bytes_to_write_to_cache = 1000000000;

SELECT count() > 0 FROM system.columns_cache WHERE database = currentDatabase();

DROP TABLE t_cc_patch_estimate;
