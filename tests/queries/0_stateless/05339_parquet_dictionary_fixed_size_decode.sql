-- Tags: no-fasttest
-- Dictionary-encoded columns of every fixed value size, read with block sizes that split RLE and bit-packed runs.

CREATE VIEW src AS
SELECT
    number AS n,
    toUInt8(h % 256) AS u8,
    toInt16(h) AS i16,
    toFixedString(leftPad(toString(h), 3, '0'), 3) AS fs3,
    toUInt32(h * 4000037) AS u32,
    toInt64(h * 1000000007) AS i64,
    toDecimal128(h, 2) AS d128,
    toFixedString(leftPad(toString(h), 5, '0'), 5) AS fs5,
    if(h % 7 = 0, NULL, toInt32(h)) AS ni32
FROM
(
    SELECT number, if(cityHash64(intDiv(number, 48)) % 3 = 0, cityHash64(number), cityHash64(intDiv(number, 48))) % 1000 AS h
    FROM numbers(200000)
);

INSERT INTO FUNCTION file(currentDatabase() || '_05339.parquet') SELECT * FROM src
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_row_group_size = 50000, output_format_parquet_data_page_size = 4096;

SELECT arrayMap(c -> c.name, arrayFilter(c -> NOT has(c.encodings, 'RLE_DICTIONARY'), columns))
FROM file(currentDatabase() || '_05339.parquet', ParquetMetadata);

SELECT sum(cityHash64(n, u8)), sum(cityHash64(n, i16)), sum(cityHash64(n, fs3)), sum(cityHash64(n, u32)), sum(cityHash64(n, i64)),
    sum(cityHash64(n, d128)), sum(cityHash64(n, fs5)), sum(cityHash64(n, ni32)), countIf(ni32 IS NULL), count()
FROM src;

SELECT sum(cityHash64(n, u8)), sum(cityHash64(n, i16)), sum(cityHash64(n, fs3)), sum(cityHash64(n, u32)), sum(cityHash64(n, i64)),
    sum(cityHash64(n, d128)), sum(cityHash64(n, fs5)), sum(cityHash64(n, ni32)), countIf(ni32 IS NULL), count()
FROM file(currentDatabase() || '_05339.parquet')
SETTINGS input_format_parquet_max_block_size = 777;

SELECT sum(cityHash64(n, u8)), sum(cityHash64(n, i16)), sum(cityHash64(n, fs3)), sum(cityHash64(n, u32)), sum(cityHash64(n, i64)),
    sum(cityHash64(n, d128)), sum(cityHash64(n, fs5)), sum(cityHash64(n, ni32)), countIf(ni32 IS NULL), count()
FROM file(currentDatabase() || '_05339.parquet');

-- A bit-packed run whose index points past the end of a 3-entry dictionary of 4-byte values is rejected.
INSERT INTO FUNCTION file(currentDatabase() || '_05339_ok.parquet') SELECT toInt32(number % 3) AS x FROM numbers(3000)
SETTINGS engine_file_truncate_on_insert = 1, output_format_parquet_compression_method = 'none';

SELECT position(x, unhex('244992244992244992244992')) > 0 FROM file(currentDatabase() || '_05339_ok.parquet', 'RawBLOB', 'x String');

INSERT INTO FUNCTION file(currentDatabase() || '_05339_bad.parquet', 'RawBLOB', 'x String')
SELECT overlay(x, '\xFF', position(x, unhex('244992244992244992244992')) + 3)
FROM file(currentDatabase() || '_05339_ok.parquet', 'RawBLOB', 'x String')
SETTINGS engine_file_truncate_on_insert = 1;

SELECT sum(x) FROM file(currentDatabase() || '_05339_ok.parquet', Parquet);
SELECT sum(x) FROM file(currentDatabase() || '_05339_bad.parquet', Parquet) SETTINGS input_format_parquet_verify_checksums = 0; -- { serverError INCORRECT_DATA }

-- A 32-bit-wide bit-packed run whose first index is 0xFFFFFFFF is rejected without reading outside the dictionary.
SELECT position(x, unhex('027F244992244992244992')) > 0 FROM file(currentDatabase() || '_05339_ok.parquet', 'RawBLOB', 'x String');

INSERT INTO FUNCTION file(currentDatabase() || '_05339_wide.parquet', 'RawBLOB', 'x String')
SELECT overlay(x, unhex('2003FFFFFFFF'), position(x, unhex('027F244992244992244992')))
FROM file(currentDatabase() || '_05339_ok.parquet', 'RawBLOB', 'x String')
SETTINGS engine_file_truncate_on_insert = 1;

SELECT sum(x) FROM file(currentDatabase() || '_05339_wide.parquet', Parquet) SETTINGS input_format_parquet_verify_checksums = 0; -- { serverError INCORRECT_DATA }

DROP VIEW src;
