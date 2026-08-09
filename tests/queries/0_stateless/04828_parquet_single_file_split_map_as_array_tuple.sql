-- Tags: no-fasttest
-- no-fasttest because of Parquet

-- Regression test for the size-based single-file Parquet split gate in `StorageFile` when a
-- Parquet map column is explicitly requested as `Array(Tuple(...))`. In that mode
-- (`SchemaContext::MapTupleAsPlainTuple`) the reader drops the `key_value` wrapper but keeps the
-- footer element names, so the value leaf is addressed as `m.value` — not the `m.values`
-- spelling `DataTypeMap` uses. Before the fix the projected-read-size estimate normalized map
-- leaves only to the `keys` / `values` spelling, so a heavy `m.value` read matched no footer
-- chunk, the estimate collapsed to 0 bytes, and the file stayed single-source even though the
-- query reads megabytes.

-- 40 row groups; the map keys are tiny while the map values (~4 MB) are incompressible random
-- data of fixed per-row length, so the checksum queries below are deterministic.
INSERT INTO FUNCTION file(concat(currentDatabase(), '_04828_map.parquet'))
    SELECT map('v', randomPrintableASCII(1000)) AS m
    FROM numbers(4000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

-- Control: reading only the light `m.key` subcolumn (~4 KB of one-character keys) must not
-- charge the estimate for the heavy `m.value` sibling, so the file stays single-source: no
-- `File × N` source in the pipeline.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(length(arrayStringConcat(m.key)))
    FROM file(concat(currentDatabase(), '_04828_map.parquet'), Parquet, 'm Array(Tuple(key String, value String))')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- Reading the heavy `m.value` subcolumn (~4 MB) must be charged for it and split (`File × N`
-- appears), even though its raw footer path is `m.key_value.value` and the `Map` naming mode
-- would call it `m.values`.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(length(arrayStringConcat(m.value)))
    FROM file(concat(currentDatabase(), '_04828_map.parquet'), Parquet, 'm Array(Tuple(key String, value String))')
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- The results must be identical whether the size gate splits the file or not. The keys are read
-- through the whole column here because the Parquet readers do not implement a direct read of
-- the `key` element of a map requested as `Array(Tuple(...))` — the format treats `m.key` as a
-- missing column and fills defaults (see #113976; `m.value` alone is read correctly) — and the
-- gate checks above only need the names to reach the split estimator, which `EXPLAIN PIPELINE`
-- exercises.
SELECT sum(length(arrayStringConcat(arrayMap(t -> t.key, m)))), sum(length(arrayStringConcat(m.value)))
    FROM file(concat(currentDatabase(), '_04828_map.parquet'), Parquet, 'm Array(Tuple(key String, value String))')
    SETTINGS max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000;
SELECT sum(length(arrayStringConcat(arrayMap(t -> t.key, m)))), sum(length(arrayStringConcat(m.value)))
    FROM file(concat(currentDatabase(), '_04828_map.parquet'), Parquet, 'm Array(Tuple(key String, value String))')
    SETTINGS max_threads = 1;
