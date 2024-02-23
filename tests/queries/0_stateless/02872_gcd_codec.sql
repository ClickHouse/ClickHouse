-- GCD codec can't be used stand-alone
CREATE TEMPORARY TABLE table_gcd_codec (n UInt64 CODEC(GCD)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

-- GCD codec rejects non-integer/decimal/datetime types
CREATE TEMPORARY TABLE table_gcd_codec (str String CODEC(GCD, LZ4)) ENGINE = Memory; -- { serverError BAD_ARGUMENTS }

-- Basic random-based correctness test
CREATE TEMPORARY TABLE table_lz4 (id UInt64, ui UInt256 CODEC(LZ4)) ENGINE = Memory;
INSERT INTO table_lz4 SELECT * FROM generateRandom() LIMIT 50;

CREATE TEMPORARY TABLE table_gcd (id UInt64, ui UInt256 CODEC(GCD, LZ4)) ENGINE = Memory;
INSERT INTO table_gcd SELECT * FROM table_lz4;

SELECT COUNT(*)
FROM (
    SELECT table_lz4.id, table_lz4.ui AS ui1, table_gcd.id, table_gcd.ui AS ui2
    FROM table_lz4 JOIN table_gcd
        ON table_lz4.id = table_gcd.id
)
WHERE ui1 != ui2;

-------------------------------------------------------------------------------------------
-- Compression/decompression works for all data types supported by GCD codec

-- Int*
CREATE TEMPORARY TABLE table_gcd_codec_uint8 (n UInt8 CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_uint16 (n UInt16 CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_uint32 (n UInt32 CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_uint64 (n UInt64 CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_uint128 (n UInt128 CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_uint256 (n UInt256 CODEC(GCD, LZ4)) ENGINE = Memory;

INSERT INTO table_gcd_codec_uint8 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_uint16 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_uint32 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_uint64 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_uint128 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_uint256 SELECT number FROM system.numbers LIMIT 50;

SELECT * FROM table_gcd_codec_uint8;
SELECT * FROM table_gcd_codec_uint16;
SELECT * FROM table_gcd_codec_uint32;
SELECT * FROM table_gcd_codec_uint64;
SELECT * FROM table_gcd_codec_uint128;
SELECT * FROM table_gcd_codec_uint256;

-- UInt*
CREATE TEMPORARY TABLE table_gcd_codec_int8 (n Int8 CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_int16 (n Int16 CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_int32 (n Int32 CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_int64 (n Int64 CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_int128 (n Int128 CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_int256 (n Int256 CODEC(GCD, LZ4)) ENGINE = Memory;

INSERT INTO table_gcd_codec_int8 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_int16 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_int32 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_int64 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_int128 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_int256 SELECT number FROM system.numbers LIMIT 50;

SELECT * FROM table_gcd_codec_int8;
SELECT * FROM table_gcd_codec_int16;
SELECT * FROM table_gcd_codec_int32;
SELECT * FROM table_gcd_codec_int64;
SELECT * FROM table_gcd_codec_int128;
SELECT * FROM table_gcd_codec_int256;

-- Decimal*
CREATE TEMPORARY TABLE table_gcd_codec_decimal32 (n Decimal32(1) CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_decimal64 (n Decimal64(1) CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_decimal128 (n Decimal128(1) CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_decimal256 (n Decimal256(1) CODEC(GCD, LZ4)) ENGINE = Memory;

INSERT INTO table_gcd_codec_decimal32 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_decimal64 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_decimal128 SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_decimal256 SELECT number FROM system.numbers LIMIT 50;

SELECT * FROM table_gcd_codec_decimal32;
SELECT * FROM table_gcd_codec_decimal64;
SELECT * FROM table_gcd_codec_decimal128;
SELECT * FROM table_gcd_codec_decimal256;

-- Date[32]
CREATE TEMPORARY TABLE table_gcd_codec_date (n Date CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_date32 (n Date32 CODEC(GCD, LZ4)) ENGINE = Memory;

INSERT INTO table_gcd_codec_date SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_date32 SELECT number FROM system.numbers LIMIT 50;

SELECT * FROM table_gcd_codec_date;
SELECT * FROM table_gcd_codec_date32;

-- DateTimeTime[64]
CREATE TEMPORARY TABLE table_gcd_codec_datetime (n DateTime('Asia/Istanbul') CODEC(GCD, LZ4)) ENGINE = Memory;
CREATE TEMPORARY TABLE table_gcd_codec_datetime64 (n DateTime64(3, 'Asia/Istanbul') CODEC(GCD, LZ4)) ENGINE = Memory;

INSERT INTO table_gcd_codec_datetime SELECT number FROM system.numbers LIMIT 50;
INSERT INTO table_gcd_codec_datetime64 SELECT number FROM system.numbers LIMIT 50;

SELECT * FROM table_gcd_codec_datetime;
SELECT * FROM table_gcd_codec_datetime64;


-- A column with all zero values can be compressed/decompressed

CREATE TEMPORARY TABLE table_gcd_codec_only_zeros (n UInt8 CODEC(GCD, LZ4)) ENGINE = Memory;
INSERT INTO table_gcd_codec_only_zeros VALUES (0), (0), (0);
SELECT * FROM table_gcd_codec_only_zeros;

-- Tests for Bug #56672:

DROP TABLE IF EXISTS table_gcd_codec_one_hundred_zeros;
DROP TABLE IF EXISTS table_gcd_codec_one_hundred_ones;

CREATE TABLE table_gcd_codec_one_hundred_zeros (a Nullable(Int64) CODEC (GCD,LZ4)) ENGINE=MergeTree ORDER BY ();
INSERT INTO table_gcd_codec_one_hundred_zeros SELECT 0 FROM numbers(100);
SELECT * FROM table_gcd_codec_one_hundred_zeros;

CREATE TABLE table_gcd_codec_one_hundred_ones (a Nullable(Int64) CODEC (GCD,LZ4)) ENGINE=MergeTree Order by ();
INSERT INTO table_gcd_codec_one_hundred_ones SELECT 1 FROM numbers(100);
SELECT * FROM table_gcd_codec_one_hundred_ones;

DROP TABLE table_gcd_codec_one_hundred_zeros;
DROP TABLE table_gcd_codec_one_hundred_ones;
