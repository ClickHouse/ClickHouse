-- Tags: no-fasttest, no-parallel
-- no-fasttest because of Parquet
-- no-parallel because we're writing a file with a fixed name

-- Regression test for the compatibility contract of the size-based single-file Parquet split gate:
-- `input_format_parquet_min_bytes_to_split = 0` together with
-- `input_format_parquet_bytes_per_split_bucket = 0` (the values `compatibility` set to a pre-26.8
-- version restores) must reproduce the pre-gate fan-out, which was driven by the row-group count
-- alone. The minimum-row-groups-per-bucket floor is part of the same heuristic, so it must be off as
-- well - otherwise a file with fewer row groups than the floor stays single-source no matter what
-- the settings say, and the settings cannot opt out of the heuristic.

-- 8 row groups (fewer than the floor), ~4 MB of incompressible data.
INSERT INTO FUNCTION file('04812.parquet')
    SELECT number AS k, randomPrintableASCII(5000) AS big FROM numbers(800)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

-- With the size heuristic enabled (any non-zero threshold), the floor keeps a file with fewer than
-- 16 row groups per bucket single-source: no multiplied `File × N` source in the pipeline.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(length(big)) FROM file('04812.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000, input_format_parquet_bytes_per_split_bucket = 1000
) WHERE explain LIKE '%File ×%';

-- With the heuristic fully disabled, the same file is split by row-group count alone.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(length(big)) FROM file('04812.parquet')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 0, input_format_parquet_bytes_per_split_bucket = 0
) WHERE explain LIKE '%File ×%';

-- The result must not depend on whether the file was split.
SELECT sum(length(big)) FROM file('04812.parquet')
    SETTINGS max_threads = 8, parallelize_output_from_storages = 1,
        input_format_parquet_min_bytes_to_split = 0, input_format_parquet_bytes_per_split_bucket = 0;
SELECT sum(length(big)) FROM file('04812.parquet')
    SETTINGS max_threads = 8, parallelize_output_from_storages = 0;
