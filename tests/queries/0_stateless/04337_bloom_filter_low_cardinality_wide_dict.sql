-- Tags: no-random-settings, long
-- Bloom filter over a LowCardinality(String) column whose single insert block has
-- more than 65536 distinct values, so the index positions are UInt32 (not UInt8/UInt16).
-- Exercises the UInt32 branch of ColumnLowCardinality::getDistinctIndexes used by the
-- distinct-hash bloom filter aggregator. force_data_skipping_indices makes the test fail
-- if the index drops a matching row.

DROP TABLE IF EXISTS bf_lc_wide_dict;
CREATE TABLE bf_lc_wide_dict
(
    id UInt64,
    s LowCardinality(String),
    INDEX idx_s s TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192;

INSERT INTO bf_lc_wide_dict
SELECT number, concat('k', toString(number)) FROM numbers(100000)
SETTINGS max_insert_threads = 1, max_insert_block_size = 200000, min_insert_block_size_rows = 200000, min_insert_block_size_bytes = 0;

SELECT 'present first';
SELECT count() FROM bf_lc_wide_dict WHERE s = 'k0' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'present mid';
SELECT count() FROM bf_lc_wide_dict WHERE s = 'k50000' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'present last';
SELECT count() FROM bf_lc_wide_dict WHERE s = 'k99999' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'present in';
SELECT count() FROM bf_lc_wide_dict WHERE s IN ('k1', 'k99998') SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'absent';
SELECT count() FROM bf_lc_wide_dict WHERE s = 'nope' SETTINGS force_data_skipping_indices = 'idx_s';
SELECT 'total';
SELECT count() FROM bf_lc_wide_dict;

DROP TABLE bf_lc_wide_dict;
