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

-- The queries above cover the cache-read side. The cache-write side needs its own
-- guard: an unfiltered bucketed *data* read (not `need_only_count`, no filter) would
-- otherwise call `addNumRowsToCache` once per bucket and store a single bucket's
-- partial count under the whole-file key. We use a fresh file so its count cache
-- starts empty, run an unfiltered bucketed read (`SELECT sum(number)`, which is split
-- across buckets), and then assert that a cached `SELECT count()` still returns the
-- true total rather than a poisoned per-bucket count. Without the `!file_bucket_info`
-- guard on `addNumRowsToCache`, the final `count()` would read a partial cached value.
INSERT INTO FUNCTION file('04230_write.parquet') SELECT * FROM numbers(3200)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50;

SELECT sum(number) FROM file('04230_write.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8, optimize_count_from_files = 0, use_cache_for_count_from_files = 1;
SELECT count() FROM file('04230_write.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8, optimize_count_from_files = 1, use_cache_for_count_from_files = 1;
