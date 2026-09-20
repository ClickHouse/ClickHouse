SET enable_lightweight_update = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_lwu_subcol_idx;

-- Regression: when a skip index is built on a subcolumn (e.g. `tup.x`) and a
-- lightweight UPDATE writes the parent column (`tup`), the stored index data
-- becomes stale for patched rows. The `canUseIndex` check must invalidate the
-- index for those parts; otherwise the granules covering updated rows would be
-- skipped and the query would return wrong results.

CREATE TABLE t_lwu_subcol_idx
(
    id UInt64,
    tup Tuple(x String, y String),
    INDEX idx_x (tup.x) TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0,
         enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_subcol_idx SELECT number, ('x' || toString(number), 'y' || toString(number)) FROM numbers(10000);

-- Baseline: 'xtarget' does not exist in the original data.
SELECT count() FROM t_lwu_subcol_idx WHERE tup.x = 'xtarget';

-- UPDATE the parent tuple column. The bloom_filter on subcolumn `tup.x` is now stale.
UPDATE t_lwu_subcol_idx SET tup = ('xtarget', 'whatever') WHERE id >= 5000 AND id < 5050;

SELECT count() FROM t_lwu_subcol_idx WHERE tup.x = 'xtarget' SETTINGS apply_patch_parts = 1;

-- Force using the bloom_filter on the subcolumn: must still return correct result
-- because the index on a subcolumn of the updated parent column must not be applied.
SELECT count() FROM t_lwu_subcol_idx WHERE tup.x = 'xtarget'
SETTINGS apply_patch_parts = 1, force_data_skipping_indices = 'idx_x';

DROP TABLE t_lwu_subcol_idx;
