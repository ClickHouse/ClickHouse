-- Tags: no-fasttest, no-parallel
-- no-fasttest because of Parquet
-- no-parallel because we're writing files with fixed names

-- Pins the minimum-row-groups-per-bucket floor of the single-file Parquet split in `StorageFile`.
-- The floor is unconditional: it is not a size-based gate, so setting
-- `input_format_parquet_min_bytes_to_split` and `input_format_parquet_bytes_per_split_bucket` to 0
-- (which is also what an older `compatibility` does) removes the size-based bounds but keeps this
-- one. A file with too few row groups is read by a single source no matter how many threads are
-- available, because splitting it multiplies the per-bucket metadata-parse and prefetcher-setup
-- cost without giving each source enough work to amortise it (see `computeBucketsByCount`).

-- 1000 rows at row-group size 50 => 20 row groups, below the floor (16 row groups per bucket, so
-- 20 row groups allow only one bucket).
INSERT INTO FUNCTION file('04814_few_row_groups.parquet') SELECT * FROM numbers(1000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50;

-- 3200 rows at row-group size 50 => 64 row groups, enough for several buckets.
INSERT INTO FUNCTION file('04814_many_row_groups.parquet') SELECT * FROM numbers(3200)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50;

-- No `File × N` source: a single source reads the whole file despite `max_threads = 8` and both
-- byte settings disabled.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT * FROM file('04814_few_row_groups.parquet')
    SETTINGS max_threads = 8, parallelize_output_from_storages = 1,
        input_format_parquet_min_bytes_to_split = 0, input_format_parquet_bytes_per_split_bucket = 0
) WHERE explain LIKE '%File ×%';

-- Sanity check that the settings above do permit a split: the same query over a file with enough
-- row groups is fanned out into per-bucket sources, so the result above is the floor at work and
-- not some other gate.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT * FROM file('04814_many_row_groups.parquet')
    SETTINGS max_threads = 8, parallelize_output_from_storages = 1,
        input_format_parquet_min_bytes_to_split = 0, input_format_parquet_bytes_per_split_bucket = 0
) WHERE explain LIKE '%File ×%';

-- Either way the result is complete.
SELECT count() FROM file('04814_few_row_groups.parquet')
    SETTINGS max_threads = 8, input_format_parquet_min_bytes_to_split = 0,
        input_format_parquet_bytes_per_split_bucket = 0;
SELECT count() FROM file('04814_many_row_groups.parquet')
    SETTINGS max_threads = 8, input_format_parquet_min_bytes_to_split = 0,
        input_format_parquet_bytes_per_split_bucket = 0;
