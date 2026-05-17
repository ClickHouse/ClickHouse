-- Tags: no-fasttest, no-parallel
-- no-fasttest because of Parquet
-- no-parallel because we're writing a file with a fixed name

-- Regression test for `SELECT count()` over a single Parquet file that gets
-- split into multiple bucketed sources by `StorageFile`. The count cache is
-- keyed by file path; if it is consulted (or written to) on the bucketed
-- read path, every source reports the file's full total and the result is
-- multiplied by the number of buckets.

-- The file must have enough row groups for `ParquetBucketSplitter` to actually
-- split it: with `min_row_groups_per_chunk = 16` and at least 2 chunks needed,
-- the file needs >= 32 row groups. We use 64 row groups (numbers(3200) at
-- row-group size 50) so the file is fanned out into multiple per-bucket
-- sources and the bucketed read path is exercised.

INSERT INTO FUNCTION file('04230.parquet') SELECT * FROM numbers(3200)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50;

SELECT count() FROM file('04230.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8, optimize_count_from_files = 1, use_cache_for_count_from_files = 1;
SELECT count() FROM file('04230.parquet') WHERE number % 7 = 0
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8, optimize_count_from_files = 1, use_cache_for_count_from_files = 1;
SELECT count() FROM file('04230.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8, optimize_count_from_files = 1, use_cache_for_count_from_files = 1;
SELECT count() FROM file('04230.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 1, optimize_count_from_files = 1, use_cache_for_count_from_files = 1;
