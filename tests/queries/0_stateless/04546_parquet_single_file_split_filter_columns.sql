-- Tags: no-fasttest, no-parallel
-- no-fasttest because of Parquet
-- no-parallel because we're writing a file with a fixed name

-- Regression test for the size-based single-file Parquet split gate in `StorageFile`:
-- the projected-read-size estimate must (1) include columns used only as `PREWHERE`
-- inputs and (2) attribute each subcolumn read to the leaf the reader actually reads,
-- so a narrow subcolumn read is charged only for its own leaf, not its siblings.
-- Before the fix `PREWHERE`-only inputs were dropped from the estimate and every
-- subcolumn was attributed to its whole top-level column, so `sum(t.x)` was charged
-- for the heavy `t.s` sibling and split anyway, defeating the gate.

-- 40 row groups; `k` and `t.x` are tiny (~32 KB of UInt64), while `big` and `t.s`
-- are ~4 MB each of incompressible random data.
INSERT INTO FUNCTION file('04546.parquet')
    SELECT
        number AS k,
        randomPrintableASCII(1000) AS big,
        (number, randomPrintableASCII(1000))::Tuple(x UInt64, s String) AS t
    FROM numbers(4000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

-- Control: a narrow read projects only `k` (far below the 1 MB floor), so the
-- file must stay single-source: no multiplied `File × N` source in the pipeline.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(k) FROM file('04546.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- The same narrow projection filtered by the big column has to scan `big`, so the
-- projected read size is ~4 MB and the file must be split (`File × N` appears).
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(k) FROM file('04546.parquet') PREWHERE length(big) = 1000
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- A read of the light tuple element `t.x` touches only the `t.x` leaf (~32 KB), well
-- below the floor, so the file must stay single-source — the estimate must not charge
-- it for the heavy `t.s` sibling.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(t.x) FROM file('04546.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- A read of the heavy tuple element `t.s` touches only the `t.s` leaf (~4 MB), above
-- the floor, so the file must be split — proving the estimate is per-leaf, not per
-- top-level column.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(length(t.s)) FROM file('04546.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- The results must be identical whether the size gate splits the file or not.
SELECT sum(k) FROM file('04546.parquet') PREWHERE length(big) = 1000
    SETTINGS max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000;
SELECT sum(k) FROM file('04546.parquet') PREWHERE length(big) = 1000
    SETTINGS max_threads = 8, parallelize_output_from_storages = 0;
SELECT sum(t.x) FROM file('04546.parquet')
    SETTINGS max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000;
SELECT sum(t.x) FROM file('04546.parquet')
    SETTINGS max_threads = 8, parallelize_output_from_storages = 0;
SELECT sum(length(t.s)) FROM file('04546.parquet')
    SETTINGS max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000;
SELECT sum(length(t.s)) FROM file('04546.parquet')
    SETTINGS max_threads = 8, parallelize_output_from_storages = 0;
