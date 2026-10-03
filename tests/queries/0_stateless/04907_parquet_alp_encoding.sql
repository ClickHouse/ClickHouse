-- Tags: no-fasttest
-- Round-trip test for the Parquet ALP encoding (opt-in via output_format_parquet_enable_alp).
-- Each row stores its key `n`, so decoded values are checked ROW BY ROW against the ground truth
-- derived from `n` (not an aggregate) -- an order bug or compensating corruptions cannot pass.
-- Covers nulls (def_count != data_count) and selective reads (AlpDecoder skip / filtered decode).

-- 1) Bulk non-null Float64 / Float32: every row must equal the value derived from its own key.
INSERT INTO FUNCTION file('04907_alp.parquet', Parquet)
SELECT number AS n, number / 100.0 AS d, toFloat32(number) / 8 AS f
FROM numbers(100000)
SETTINGS output_format_parquet_enable_alp = 1, output_format_parquet_compression_method = 'none', engine_file_truncate_on_insert = 1;

SELECT
    countIf(reinterpretAsUInt64(d) != reinterpretAsUInt64(n / 100.0)) AS d_mismatches,
    countIf(reinterpretAsUInt32(f) != reinterpretAsUInt32(toFloat32(n) / 8)) AS f_mismatches
FROM file('04907_alp.parquet', Parquet);

-- 2) Selective reads exercise the filtered-decode and skip paths in AlpDecoder.
SELECT countIf(reinterpretAsUInt64(d) != reinterpretAsUInt64(n / 100.0))
FROM file('04907_alp.parquet', Parquet) WHERE n % 10 = 0;

SELECT countIf(reinterpretAsUInt64(d) != reinterpretAsUInt64(n / 100.0))
FROM file('04907_alp.parquet', Parquet) WHERE n >= 99000;

-- 3) Nullable column: NULLs make the page count all rows while the ALP payload stores only the
--    present values, so this exercises the def-level path together with ALP decoding.
INSERT INTO FUNCTION file('04907_alp_nullable.parquet', Parquet)
SELECT number AS n, if(number % 7 = 0, NULL, number / 100.0) AS d
FROM numbers(100000)
SETTINGS output_format_parquet_enable_alp = 1, output_format_parquet_compression_method = 'none', engine_file_truncate_on_insert = 1;

SELECT
    countIf(d IS NULL) > 0 AS has_nulls,
    countIf(d IS NOT NULL AND reinterpretAsUInt64(assumeNotNull(d)) != reinterpretAsUInt64(n / 100.0)) AS mismatches
FROM file('04907_alp_nullable.parquet', Parquet);
