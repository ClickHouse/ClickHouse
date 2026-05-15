-- Tags: no-fasttest, no-parallel
-- no-fasttest because of Parquet
-- no-parallel because we're writing a file with a fixed name

-- Regression test for the `parallelize_output_from_storages = 0` contract on the
-- single-file Parquet split path in `StorageFile`. The bucketed read path creates
-- one source per row-group chunk, which is exactly the kind of read parallelism
-- the setting forbids: with `parallelize_output_from_storages = 0`, we must keep
-- a single `File` source even when `max_threads > 1` and the file has enough row
-- groups to otherwise be split.

INSERT INTO FUNCTION file('04238.parquet') SELECT * FROM numbers(2000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50;

-- The file has 40 row groups, above the per-chunk floor in the splitter, so the
-- bucketed path can be taken when permitted.

-- With parallelize_output_from_storages = 0 and max_threads = 8 the pipeline must
-- contain a single (non-multiplied) `File 0 -> 1` source and no `Resize 1 -> N`.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT * FROM file('04238.parquet')
    SETTINGS parallelize_output_from_storages = 0, max_threads = 8
) WHERE explain LIKE '%File ×%';

SELECT count() FROM (
    EXPLAIN PIPELINE SELECT * FROM file('04238.parquet')
    SETTINGS parallelize_output_from_storages = 0, max_threads = 8
) WHERE explain LIKE '%Resize %';

-- Sanity check: with parallelize_output_from_storages = 1 the same file IS split
-- into multiple per-bucket sources (`File × N 0 -> 1`).
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT * FROM file('04238.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8
) WHERE explain LIKE '%File ×%';

-- The result must be the same regardless of the setting.
SELECT count() FROM file('04238.parquet')
    SETTINGS parallelize_output_from_storages = 0, max_threads = 8;
SELECT count() FROM file('04238.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8;
