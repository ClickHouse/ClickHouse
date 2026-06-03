SET enable_lightweight_update = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_lwu_update_indexes;

CREATE TABLE t_lwu_update_indexes
(
    key UInt64,
    value String,
    INDEX idx_key (key) TYPE minmax GRANULARITY 1,
    INDEX idx_value (value) TYPE bloom_filter(0.001) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0, enable_block_number_column = 1, enable_block_offset_column = 1;

INSERT INTO t_lwu_update_indexes SELECT number, 'v' || toString(number) FROM numbers(10000);

-- Baseline: 'vtarget' does not exist
SELECT count() FROM t_lwu_update_indexes WHERE value = 'vtarget';

-- Lightweight UPDATE changes the bloom_filter-indexed column for a range of rows.
-- The stored bloom_filter does not include 'vtarget' for those marks, so a query using
-- the stale index may incorrectly skip granules containing the updated rows.
UPDATE t_lwu_update_indexes SET value = 'vtarget' WHERE key >= 5000 AND key < 5050;

-- With patches applied the updated rows must be visible.
SELECT count() FROM t_lwu_update_indexes WHERE value = 'vtarget' SETTINGS apply_patch_parts = 1;

-- Force using the bloom_filter index: must still return correct result because the
-- index on the updated column must not be used against the original data.
SELECT count() FROM t_lwu_update_indexes WHERE value = 'vtarget'
SETTINGS apply_patch_parts = 1, force_data_skipping_indices = 'idx_value';

-- Query on non-updated column stays correct: the minmax index on `key` is still valid.
SELECT count() FROM t_lwu_update_indexes WHERE key = 5010 SETTINGS apply_patch_parts = 1;

SELECT count() FROM t_lwu_update_indexes WHERE key = 5010
SETTINGS apply_patch_parts = 1, force_data_skipping_indices = 'idx_key';

DROP TABLE t_lwu_update_indexes;
