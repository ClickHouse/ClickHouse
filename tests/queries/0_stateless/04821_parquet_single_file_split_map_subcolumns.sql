-- Tags: no-fasttest
-- no-fasttest because of Parquet

-- Regression test for the size-based single-file Parquet split gate in `StorageFile` with direct
-- `Map` subcolumn reads. The reader renames the map tuple elements (raw footer names `key` /
-- `value`) to the `keys` / `values` subcolumn names `DataTypeMap` requires, so a direct read of
-- `m.values` requests the logical name `m.values`. Before the fix the projected-read-size
-- estimate normalized the footer leaves to `m.key` / `m.value` instead, so a heavy `m.values`
-- read matched no footer chunk, the estimate collapsed to 0 bytes, and the file stayed
-- single-source even though the query reads megabytes. Whole-map access (`m['v']`) was not
-- affected: it requests the top-level name `m`, which is a dotted prefix of both leaves.

-- 40 row groups; `k` and the map keys are tiny, while the map values (~4 MB) and the values of
-- the map nested inside the tuple `t` (~4 MB) are incompressible random data of fixed per-row
-- length, so the checksum queries below are deterministic.
INSERT INTO FUNCTION file(concat(currentDatabase(), '_04821_map.parquet'))
    SELECT
        number AS k,
        map('v', randomPrintableASCII(1000)) AS m,
        tuple(map('w', randomPrintableASCII(1000)))::Tuple(m Map(String, String)) AS t
    FROM numbers(4000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

-- Control: reading only the light `m.keys` subcolumn (~40 KB of one-character keys) must not
-- charge the estimate for the heavy `m.values` sibling, so the file stays single-source: no
-- `File × N` source in the pipeline.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(length(arrayStringConcat(m.keys))) FROM file(concat(currentDatabase(), '_04821_map.parquet'))
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- Reading the heavy `m.values` subcolumn (~4 MB) must be charged for it and split (`File × N`
-- appears), even though its raw footer path is `m.key_value.value`.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(length(arrayStringConcat(m.values))) FROM file(concat(currentDatabase(), '_04821_map.parquet'))
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- The same for a map nested inside a tuple: the heavy `t.m.values` read (raw footer path
-- `t.m.key_value.value`, ~4 MB) must be charged and the file split.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(length(arrayStringConcat(t.m.values))) FROM file(concat(currentDatabase(), '_04821_map.parquet'))
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- The results must be identical whether the size gate splits the file or not. Whole-map access
-- is used here because the Parquet readers do not implement direct `Map` subcolumn reads yet
-- (the format treats `m.keys` / `m.values` as missing columns) — the gate checks above only
-- need the names to reach the split estimator, which `EXPLAIN PIPELINE` exercises.
SELECT sum(length(arrayStringConcat(mapKeys(m)))), sum(length(m['v'])), sum(length(t.m['w']))
    FROM file(concat(currentDatabase(), '_04821_map.parquet'))
    SETTINGS max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000;
SELECT sum(length(arrayStringConcat(mapKeys(m)))), sum(length(m['v'])), sum(length(t.m['w']))
    FROM file(concat(currentDatabase(), '_04821_map.parquet'))
    SETTINGS max_threads = 1;
