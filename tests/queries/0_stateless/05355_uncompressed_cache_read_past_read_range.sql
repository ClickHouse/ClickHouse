-- Tags: no-fasttest
-- no-fasttest: needs the S3 disk

-- A read of the first granule looks past the end of its range (the scan-time string filter checks for more
-- data before the empty values). It must not leave an empty block in the uncompressed cache for the next readers.

DROP TABLE IF EXISTS t_uncompressed_cache_read_range;

CREATE TABLE t_uncompressed_cache_read_range (id UInt32, s String)
ENGINE = MergeTree ORDER BY id
SETTINGS disk = 's3_disk', index_granularity = 4, index_granularity_bytes = 10485760,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 1,
    serialization_info_version = 'with_types', string_serialization_version = 'with_size_stream',
    ratio_of_defaults_for_sparse_serialization = 1.0;

-- The first granule is one value and three empty strings; the second granule starts a new compressed block.
INSERT INTO t_uncompressed_cache_read_range
SELECT number, multiIf(number = 0, 'needle', number < 4, '', concat('value_', toString(number))) FROM numbers(8);

-- Read the first granule one row at a time (the query condition cache would round reads up to whole granules).
SELECT count() FROM t_uncompressed_cache_read_range PREWHERE s LIKE '%needle%' WHERE id < 4
SETTINGS apply_string_filters_during_scan = 1, use_uncompressed_cache = 1, max_block_size = 1, max_threads = 1,
    enable_parallel_replicas = 0, use_columns_cache = 0, use_query_condition_cache = 0;

SELECT id, s FROM t_uncompressed_cache_read_range WHERE id >= 4 ORDER BY id
SETTINGS apply_string_filters_during_scan = 0, use_uncompressed_cache = 1, max_threads = 1, enable_parallel_replicas = 0;

DROP TABLE t_uncompressed_cache_read_range;
