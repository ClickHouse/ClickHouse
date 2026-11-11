-- Tags: no-fasttest
-- Columns of PLAIN-encoded fixed-size Parquet pages read after PREWHERE steps, compared with the source table.
-- No page index and no dictionary, so the row filter is applied while decoding; small pages that straddle
-- the 1000-row blocks; value sizes 1, 2, 3, 4, 8, 16 and 32 bytes, with and without a type conversion.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS pq;

CREATE TABLE src ENGINE = Memory AS SELECT
    toUInt32(number) AS k,
    toUInt8(number % 4) AS x,
    toUInt8(cityHash64(number, 1)) AS c_u8,
    toInt8(cityHash64(number, 2)) AS c_i8,
    toInt16(cityHash64(number, 3)) AS c_i16,
    toUInt16(cityHash64(number, 4)) AS c_u16,
    toInt32(cityHash64(number, 5)) AS c_i32,
    toInt64(cityHash64(number, 6)) AS c_i64,
    cityHash64(number, 7) AS c_u64,
    toInt128(cityHash64(number, 8)) * toInt128(cityHash64(number, 9)) - toInt128(cityHash64(number, 10)) AS c_i128,
    toUInt256(cityHash64(number, 11)) * cityHash64(number, 12) * cityHash64(number, 13) AS c_u256,
    toFloat32(cityHash64(number, 14) % 1000000 / 7) AS c_f32,
    toFloat64(cityHash64(number, 15)) / 3 AS c_f64,
    toDate(cityHash64(number, 16) % 40000) AS c_date,
    toDate32(toInt32(cityHash64(number, 17) % 100000) - 30000) AS c_date32,
    toDateTime(cityHash64(number, 18) % 4000000000, 'UTC') AS c_dt,
    toDateTime64(cityHash64(number, 19) % 4000000000000, 3, 'UTC') AS c_dt64,
    toDecimal32(toInt32(cityHash64(number, 20) % 1999999) - 999999, 3) AS c_d32,
    toDecimal64(toInt64(cityHash64(number, 21) % 1999999999999) - 999999999999, 5) AS c_d64,
    toDecimal128(toInt128(cityHash64(number, 22)) * 1000000, 10) AS c_d128,
    toDecimal256(toInt256(cityHash64(number, 23)) * toInt256(cityHash64(number, 24)), 20) AS c_d256,
    toFixedString(substring(hex(cityHash64(number, 25)), 1, 1), 1) AS c_fs1,
    toFixedString(substring(hex(cityHash64(number, 26)), 1, 2), 2) AS c_fs2,
    toFixedString(substring(hex(cityHash64(number, 27)), 1, 3), 3) AS c_fs3,
    toFixedString(hex(cityHash64(number, 28)), 16) AS c_fs16,
    reinterpretAsUUID(reinterpretAsFixedString(toUInt128(cityHash64(number, 29)) * cityHash64(number, 30))) AS c_uuid
FROM numbers(5003);

-- A File table takes its format settings from its own SETTINGS clause, not from the query.
CREATE TABLE pq AS src ENGINE = File(Parquet) SETTINGS
    output_format_parquet_write_page_index = 0,
    output_format_parquet_max_dictionary_size = 0,
    output_format_parquet_data_page_size = 1000,
    output_format_parquet_batch_size = 97,
    input_format_parquet_max_block_size = 1000;
INSERT INTO pq SELECT * FROM src;

SELECT 'every row';
SELECT count(), sum(cityHash64(*)) FROM pq PREWHERE k < 4294967295;
SELECT count(), sum(cityHash64(*)) FROM src WHERE k < 4294967295;

SELECT 'no row';
SELECT count(), sum(cityHash64(*)) FROM pq PREWHERE k > 4294967294;
SELECT count(), sum(cityHash64(*)) FROM src WHERE k > 4294967294;

SELECT 'all but one row';
SELECT count(), sum(cityHash64(*)) FROM pq PREWHERE k != 2345;
SELECT count(), sum(cityHash64(*)) FROM src WHERE k != 2345;

SELECT 'half, scattered';
SELECT count(), sum(cityHash64(*)) FROM pq PREWHERE cityHash64(k) % 2 = 0;
SELECT count(), sum(cityHash64(*)) FROM src WHERE cityHash64(k) % 2 = 0;

SELECT '0.1%';
SELECT count(), sum(cityHash64(*)) FROM pq PREWHERE k % 1000 = 0;
SELECT count(), sum(cityHash64(*)) FROM src WHERE k % 1000 = 0;

SELECT '99.9%';
SELECT count(), sum(cityHash64(*)) FROM pq PREWHERE k % 1000 != 0;
SELECT count(), sum(cityHash64(*)) FROM src WHERE k % 1000 != 0;

SELECT 'runs of 1500 rows';
SELECT count(), sum(cityHash64(*)) FROM pq PREWHERE intDiv(k, 1500) % 2 = 0;
SELECT count(), sum(cityHash64(*)) FROM src WHERE intDiv(k, 1500) % 2 = 0;

SELECT 'UInt8 filter with values above 1';
SELECT count(), sum(cityHash64(*)) FROM pq PREWHERE x;
SELECT count(), sum(cityHash64(*)) FROM src WHERE x;

SELECT 'two steps, the first passes every row';
SELECT count(), sum(cityHash64(*)) FROM pq PREWHERE k < 4294967295 AND x != 0;
SELECT count(), sum(cityHash64(*)) FROM src WHERE k < 4294967295 AND x != 0;

SELECT 'two steps, the second passes every row';
SELECT count(), sum(cityHash64(*)) FROM pq PREWHERE cityHash64(k) % 3 = 0 AND x < 4;
SELECT count(), sum(cityHash64(*)) FROM src WHERE cityHash64(k) % 3 = 0 AND x < 4;

SELECT 'two steps, the second passes every row, PREWHERE-only columns';
SELECT count(), sum(cityHash64(COLUMNS('^c_'))) FROM pq PREWHERE cityHash64(k) % 3 = 0 AND x < 4;
SELECT count(), sum(cityHash64(COLUMNS('^c_'))) FROM src WHERE cityHash64(k) % 3 = 0 AND x < 4;

DROP TABLE pq;
DROP TABLE src;
