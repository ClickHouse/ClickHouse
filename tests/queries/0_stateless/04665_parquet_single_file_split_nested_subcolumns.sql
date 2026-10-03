-- Tags: no-fasttest
-- no-fasttest because of Parquet

-- Regression test for the size-based single-file Parquet split gate in `StorageFile` with
-- addressable nested subcolumns. The projected-read-size estimate compares the reader's logical
-- column names against the raw footer `path_in_schema`, which keeps List / Map wrapper segments
-- (`a.list.element.x` for an `Array(Tuple(...))` element addressed as `a.x`). Before the fix the
-- raw paths were compared as strings, so a heavy read of a nested leaf like `a.s` matched no
-- footer chunk, the estimate collapsed to 0 bytes, and the file stayed single-source even though
-- the query reads megabytes. The same applied to a whole inner tuple (`t.inner`), whose leaves
-- (`t.inner.x`, `t.inner.s`) match it only by dotted prefix.

-- 40 row groups; `k` and the `a.x` / `t.inner.x` leaves are tiny, while `a.s` (~3.8 MB; the
-- lambda must depend on `number`, or the whole array is evaluated once per block and dictionary
-- encoding collapses it), `t.inner.s` and the map values (~4 MB each) are incompressible random
-- data. The per-row length sum of `a.s` is deterministic (945) for the checksum queries below.
INSERT INTO FUNCTION file(concat(currentDatabase(), '_04665_nested.parquet'))
    SELECT
        number AS k,
        arrayMap(i -> (i, randomPrintableASCII(90 + ((number + i) % 10))), range(10))::Array(Tuple(x UInt64, s String)) AS a,
        tuple((number, randomPrintableASCII(1000))::Tuple(x UInt64, s String))::Tuple(inner Tuple(x UInt64, s String)) AS t,
        map('v', randomPrintableASCII(1000)) AS m
    FROM numbers(4000)
    SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

-- Control: reading the light nested leaf `a.x` (~320 KB of small integers) must not charge the
-- estimate for the heavy `a.s` sibling, so the file stays single-source: no `File × N` source in
-- the pipeline.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(arraySum(a.x)) FROM file(concat(currentDatabase(), '_04665_nested.parquet'))
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- Reading the heavy nested leaf `a.s` (~4 MB) must be charged for it and split (`File × N`
-- appears), even though its raw footer path is `a.list.element.s`, not `a.s`.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(length(arrayStringConcat(a.s))) FROM file(concat(currentDatabase(), '_04665_nested.parquet'))
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- Reading a whole inner tuple requests the name `t.inner`, which is only a dotted prefix of the
-- footer leaves `t.inner.x` / `t.inner.s`; the heavy `t.inner.s` leaf (~4 MB) must be charged and
-- the file split.
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT max(t.inner) FROM file(concat(currentDatabase(), '_04665_nested.parquet'))
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- A whole-map read requests the top-level name `m`, which must keep matching the map's footer
-- leaves (`m.key_value.key`, `m.key_value.value`) and split on the heavy values (~4 MB).
SELECT count() FROM (
    EXPLAIN PIPELINE SELECT sum(length(m['v'])) FROM file(concat(currentDatabase(), '_04665_nested.parquet'))
    SETTINGS parallelize_output_from_storages = 1, max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000
) WHERE explain LIKE '%File ×%';

-- The results must be identical whether the size gate splits the file or not.
SELECT sum(arraySum(a.x)), sum(length(arrayStringConcat(a.s))), max(t.inner.x), sum(length(m['v']))
    FROM file(concat(currentDatabase(), '_04665_nested.parquet'))
    SETTINGS max_threads = 8,
        input_format_parquet_min_bytes_to_split = 1000000, input_format_parquet_bytes_per_split_bucket = 1000000;
SELECT sum(arraySum(a.x)), sum(length(arrayStringConcat(a.s))), max(t.inner.x), sum(length(m['v']))
    FROM file(concat(currentDatabase(), '_04665_nested.parquet'))
    SETTINGS max_threads = 1;
