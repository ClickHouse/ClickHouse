-- Tags: no-fasttest
-- Round-trip test for the Parquet ALP encoding (opt-in via output_format_parquet_enable_alp).
-- Self-checking: ALP output must be bit-identical to PLAIN, so every result is 1.

-- Bulk Float64 / Float32: decimal-like data, ALP's sweet spot.
INSERT INTO FUNCTION file('03510_alp.parquet', Parquet)
SELECT number / 100.0 AS d, toFloat32(number) / 8 AS f FROM numbers(100000)
SETTINGS output_format_parquet_enable_alp = 1, engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file('03510_plain.parquet', Parquet)
SELECT number / 100.0 AS d, toFloat32(number) / 8 AS f FROM numbers(100000)
SETTINGS output_format_parquet_enable_alp = 0, engine_file_truncate_on_insert = 1;

SELECT
    (SELECT groupBitXor(reinterpretAsUInt64(d)) FROM file('03510_alp.parquet', Parquet))
     = (SELECT groupBitXor(reinterpretAsUInt64(d)) FROM file('03510_plain.parquet', Parquet)) AS double_bit_identical,
    (SELECT groupBitXor(reinterpretAsUInt32(f)) FROM file('03510_alp.parquet', Parquet))
     = (SELECT groupBitXor(reinterpretAsUInt32(f)) FROM file('03510_plain.parquet', Parquet)) AS float_bit_identical;

-- Exception path: signed zero, NaN, +/-inf, exponent-range extremes.
INSERT INTO FUNCTION file('03510_alp_edge.parquet', Parquet)
SELECT arrayJoin([0.0, -0.0, 1.5, -2.25, 1e18, 1e-18, nan, inf, -inf]::Array(Float64)) AS d
SETTINGS output_format_parquet_enable_alp = 1, engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file('03510_plain_edge.parquet', Parquet)
SELECT arrayJoin([0.0, -0.0, 1.5, -2.25, 1e18, 1e-18, nan, inf, -inf]::Array(Float64)) AS d
SETTINGS output_format_parquet_enable_alp = 0, engine_file_truncate_on_insert = 1;

SELECT
    (SELECT groupBitXor(reinterpretAsUInt64(d)) FROM file('03510_alp_edge.parquet', Parquet))
     = (SELECT groupBitXor(reinterpretAsUInt64(d)) FROM file('03510_plain_edge.parquet', Parquet)) AS edge_bit_identical;
