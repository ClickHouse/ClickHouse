-- { echo }

DROP TABLE IF EXISTS test_int64;
CREATE TABLE test_int64 (uid Int64) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 8192;
INSERT INTO test_int64 SELECT number FROM numbers(200000);

SELECT count() FROM test_int64 WHERE uid < 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int64 WHERE uid <= 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int64 WHERE uid > 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int64 WHERE uid >= 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int64 WHERE uid = 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid != 100000.5;

SELECT count() FROM test_int64 WHERE uid < 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int64 WHERE uid > 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int64 WHERE uid = 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid < -0.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid > (1.0 / 0.0) SETTINGS force_primary_key = 1, max_rows_to_read = 1;

-- Int64 exact
SELECT count() FROM test_int64 WHERE uid < 100000.0 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int64 WHERE uid <= 100000.0 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int64 WHERE uid > 100000.0 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int64 WHERE uid >= 100000.0 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int64 WHERE uid = 100000.0 SETTINGS force_primary_key = 1, max_rows_to_read = 8192;
SELECT count() FROM test_int64 WHERE uid != 100000.0 SETTINGS force_primary_key = 1, max_rows_to_read = 204800;

-- Int64 negative fractional
SELECT count() FROM test_int64 WHERE uid < -0.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid <= -0.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid > -0.5 SETTINGS force_primary_key = 1, max_rows_to_read = 204800;
SELECT count() FROM test_int64 WHERE uid >= -0.5 SETTINGS force_primary_key = 1, max_rows_to_read = 204800;
SELECT count() FROM test_int64 WHERE uid = -0.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid != -0.5;

-- Int64 +Inf
SELECT count() FROM test_int64 WHERE uid < (1.0 / 0.0);
SELECT count() FROM test_int64 WHERE uid <= (1.0 / 0.0);
SELECT count() FROM test_int64 WHERE uid > (1.0 / 0.0) SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid >= (1.0 / 0.0) SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid = (1.0 / 0.0) SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid != (1.0 / 0.0);

-- Int64 -Inf
SELECT count() FROM test_int64 WHERE uid < (-1.0 / 0.0) SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid <= (-1.0 / 0.0) SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid > (-1.0 / 0.0);
SELECT count() FROM test_int64 WHERE uid >= (-1.0 / 0.0);
SELECT count() FROM test_int64 WHERE uid = (-1.0 / 0.0) SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64 WHERE uid != (-1.0 / 0.0);

DROP TABLE IF EXISTS test_uint64;
CREATE TABLE test_uint64 (uid UInt64) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 8192;
INSERT INTO test_uint64 SELECT number FROM numbers(200000);

-- UInt64 fractional
SELECT count() FROM test_uint64 WHERE uid < 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_uint64 WHERE uid <= 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_uint64 WHERE uid > 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_uint64 WHERE uid >= 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_uint64 WHERE uid = 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64 WHERE uid != 100000.5;

-- UInt64 below min
SELECT count() FROM test_uint64 WHERE uid < -1.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64 WHERE uid <= -1.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64 WHERE uid > -1.5;
SELECT count() FROM test_uint64 WHERE uid >= -1.5;
SELECT count() FROM test_uint64 WHERE uid = -1.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64 WHERE uid != -1.5;

SELECT count() FROM test_uint64 WHERE uid < 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_uint64 WHERE uid > 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_uint64 WHERE uid < -1.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64 WHERE uid = -1.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64 WHERE uid = -0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 8192;

-- UInt64 signed zero
SELECT count() FROM test_uint64 WHERE uid < -0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64 WHERE uid <= -0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 8192;
SELECT count() FROM test_uint64 WHERE uid > -0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 204800;
SELECT count() FROM test_uint64 WHERE uid >= -0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 204800;
SELECT count() FROM test_uint64 WHERE uid = -0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 8192;
SELECT count() FROM test_uint64 WHERE uid != -0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 204800;

SELECT count() FROM test_uint64 WHERE uid < +0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64 WHERE uid <= +0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 8192;
SELECT count() FROM test_uint64 WHERE uid > +0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 204800;
SELECT count() FROM test_uint64 WHERE uid >= +0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 204800;
SELECT count() FROM test_uint64 WHERE uid = +0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 8192;
SELECT count() FROM test_uint64 WHERE uid != +0.0 SETTINGS force_primary_key = 1, max_rows_to_read = 204800;

DROP TABLE IF EXISTS test_int32;
CREATE TABLE test_int32 (uid Int32) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 8192;
INSERT INTO test_int32 SELECT number FROM numbers(200000);

-- Int32 fractional
SELECT count() FROM test_int32 WHERE uid < 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_int32 WHERE uid > 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 106496;

DROP TABLE IF EXISTS test_int8;
CREATE TABLE test_int8 (uid Int8) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 8192;
INSERT INTO test_int8 SELECT toInt8(number - 128) FROM numbers(256);

-- Int8 above max
SELECT count() FROM test_int8 WHERE uid < 200.5;
SELECT count() FROM test_int8 WHERE uid <= 200.5;
SELECT count() FROM test_int8 WHERE uid > 200.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int8 WHERE uid >= 200.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int8 WHERE uid = 200.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int8 WHERE uid != 200.5;

-- Int8 below min
SELECT count() FROM test_int8 WHERE uid < -200.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int8 WHERE uid <= -200.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int8 WHERE uid > -200.5;
SELECT count() FROM test_int8 WHERE uid >= -200.5;
SELECT count() FROM test_int8 WHERE uid = -200.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int8 WHERE uid != -200.5;

-- Int8 in-range fractional
SELECT count() FROM test_int8 WHERE uid < 50.5 SETTINGS force_primary_key = 1, max_rows_to_read = 256;
SELECT count() FROM test_int8 WHERE uid > 50.5 SETTINGS force_primary_key = 1, max_rows_to_read = 256;

DROP TABLE IF EXISTS test_uint8;
CREATE TABLE test_uint8 (uid UInt8) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 8192;
INSERT INTO test_uint8 SELECT number FROM numbers(256);

-- UInt8 above max
SELECT count() FROM test_uint8 WHERE uid < 300.5;
SELECT count() FROM test_uint8 WHERE uid > 300.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint8 WHERE uid = 300.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;

-- UInt8 in-range fractional
SELECT count() FROM test_uint8 WHERE uid < 100.5 SETTINGS force_primary_key = 1, max_rows_to_read = 256;
SELECT count() FROM test_uint8 WHERE uid > 100.5 SETTINGS force_primary_key = 1, max_rows_to_read = 256;

DROP TABLE IF EXISTS test_negation;
CREATE TABLE test_negation (uid Int64) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 8192;
INSERT INTO test_negation SELECT number FROM numbers(200000);

-- Negation
SELECT count() FROM test_negation WHERE NOT (uid < 100000.5) SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_negation WHERE NOT (uid > 100000.5) SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_negation WHERE NOT (uid = 100000.5);
SELECT count() FROM test_negation WHERE NOT (uid != 100000.5) SETTINGS force_primary_key = 1, max_rows_to_read = 1;

DROP TABLE IF EXISTS test_int64_boundary;
CREATE TABLE test_int64_boundary (uid Int64) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1;
INSERT INTO test_int64_boundary VALUES (-1), (0), (9223372036854775807);

-- Int64 max boundary
SELECT count() FROM test_int64_boundary WHERE uid < 9223372036854775808.0;
SELECT count() FROM test_int64_boundary WHERE uid >= 9223372036854775808.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;

DROP TABLE IF EXISTS test_uint64_boundary;
CREATE TABLE test_uint64_boundary (uid UInt64) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1;
INSERT INTO test_uint64_boundary VALUES (0), (9223372036854775808), (18446744073709551615);

-- UInt64 max boundary
SELECT count() FROM test_uint64_boundary WHERE uid < 18446744073709551616.0;
SELECT count() FROM test_uint64_boundary WHERE uid >= 18446744073709551616.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;

DROP TABLE IF EXISTS test_int64_boundary;
CREATE TABLE test_int64_boundary (uid Int64) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1;
INSERT INTO test_int64_boundary VALUES (-9223372036854775808), (-1), (0), (9223372036854775807);

-- Int64 above max
SELECT count() FROM test_int64_boundary WHERE uid < 9223372036854775808.0;
SELECT count() FROM test_int64_boundary WHERE uid <= 9223372036854775808.0;
SELECT count() FROM test_int64_boundary WHERE uid > 9223372036854775808.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64_boundary WHERE uid >= 9223372036854775808.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64_boundary WHERE uid = 9223372036854775808.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64_boundary WHERE uid != 9223372036854775808.0;

-- Int64 below min
SELECT count() FROM test_int64_boundary WHERE uid < -9223372036854777856.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64_boundary WHERE uid <= -9223372036854777856.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64_boundary WHERE uid > -9223372036854777856.0;
SELECT count() FROM test_int64_boundary WHERE uid >= -9223372036854777856.0;
SELECT count() FROM test_int64_boundary WHERE uid = -9223372036854777856.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64_boundary WHERE uid != -9223372036854777856.0;

DROP TABLE IF EXISTS test_uint64_boundary;
CREATE TABLE test_uint64_boundary (uid UInt64) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1;
INSERT INTO test_uint64_boundary VALUES (0), (1), (9223372036854775808), (18446744073709551615);

-- UInt64 above max
SELECT count() FROM test_uint64_boundary WHERE uid < 18446744073709551616.0;
SELECT count() FROM test_uint64_boundary WHERE uid <= 18446744073709551616.0;
SELECT count() FROM test_uint64_boundary WHERE uid > 18446744073709551616.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64_boundary WHERE uid >= 18446744073709551616.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64_boundary WHERE uid = 18446744073709551616.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint64_boundary WHERE uid != 18446744073709551616.0;

DROP TABLE IF EXISTS test_constest_left;
CREATE TABLE test_constest_left (uid Int64) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 8192;
INSERT INTO test_constest_left SELECT number FROM numbers(200000);

-- Const left Int64
SELECT count() FROM test_constest_left WHERE 100000.5 > uid SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_constest_left WHERE 100000.5 >= uid SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_constest_left WHERE 100000.5 < uid SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_constest_left WHERE 100000.5 <= uid SETTINGS force_primary_key = 1, max_rows_to_read = 106496;
SELECT count() FROM test_constest_left WHERE 100000.5 = uid SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_constest_left WHERE 100000.5 != uid;

DROP TABLE IF EXISTS test_nullable_int64;
CREATE TABLE test_nullable_int64 (uid Nullable(Int64)) ENGINE = MergeTree ORDER BY uid SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO test_nullable_int64 VALUES (NULL), (-1), (0), (100000), (100001), (9223372036854775807);

-- Nullable Int64
SELECT count() FROM test_nullable_int64 WHERE uid < 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT count() FROM test_nullable_int64 WHERE uid <= 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT count() FROM test_nullable_int64 WHERE uid > 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT count() FROM test_nullable_int64 WHERE uid >= 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 3;
SELECT count() FROM test_nullable_int64 WHERE uid = 100000.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_nullable_int64 WHERE uid != 100000.5;
SELECT count() FROM test_nullable_int64 WHERE uid < (1.0 / 0.0);
SELECT count() FROM test_nullable_int64 WHERE uid >= (1.0 / 0.0) SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_nullable_int64 WHERE uid < 9223372036854775808.0;
SELECT count() FROM test_nullable_int64 WHERE uid >= 9223372036854775808.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;

DROP TABLE IF EXISTS test_int64_2pow53;
CREATE TABLE test_int64_2pow53 (uid Int64) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1;
INSERT INTO test_int64_2pow53 VALUES (9007199254740991), (9007199254740992), (9007199254740993), (9007199254740994);

-- Int64 2^53 rounding
SELECT count() FROM test_int64_2pow53 WHERE uid = 9007199254740993.0 SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT count() FROM test_int64_2pow53 WHERE uid != 9007199254740993.0 SETTINGS force_primary_key = 1, max_rows_to_read = 4;
SELECT count() FROM test_int64_2pow53 WHERE uid < 9007199254740993.0 SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT count() FROM test_int64_2pow53 WHERE uid >= 9007199254740993.0 SETTINGS force_primary_key = 1, max_rows_to_read = 4;

DROP TABLE IF EXISTS test_int64_2pow52_fractional;
CREATE TABLE test_int64_2pow52_fractional (uid Int64) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1;
INSERT INTO test_int64_2pow52_fractional VALUES (4503599627370494), (4503599627370495), (4503599627370496), (4503599627370497);

-- Int64 2^52 fractional
SELECT count() FROM test_int64_2pow52_fractional WHERE uid = 4503599627370495.5 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int64_2pow52_fractional WHERE uid != 4503599627370495.5;
SELECT count() FROM test_int64_2pow52_fractional WHERE uid < 4503599627370495.5 SETTINGS force_primary_key = 1, max_rows_to_read = 2;
SELECT count() FROM test_int64_2pow52_fractional WHERE uid >= 4503599627370495.5 SETTINGS force_primary_key = 1, max_rows_to_read = 3;

DROP TABLE IF EXISTS test_int32_boundary;
CREATE TABLE test_int32_boundary (uid Int32) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1;
INSERT INTO test_int32_boundary VALUES (-2147483648), (-1), (0), (2147483647);

-- Int32 above max
SELECT count() FROM test_int32_boundary WHERE uid < 2147483648.0;
SELECT count() FROM test_int32_boundary WHERE uid <= 2147483648.0;
SELECT count() FROM test_int32_boundary WHERE uid > 2147483648.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int32_boundary WHERE uid >= 2147483648.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int32_boundary WHERE uid = 2147483648.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int32_boundary WHERE uid != 2147483648.0;

-- Int32 below min
SELECT count() FROM test_int32_boundary WHERE uid < -2147483649.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int32_boundary WHERE uid <= -2147483649.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int32_boundary WHERE uid > -2147483649.0;
SELECT count() FROM test_int32_boundary WHERE uid >= -2147483649.0;
SELECT count() FROM test_int32_boundary WHERE uid = -2147483649.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int32_boundary WHERE uid != -2147483649.0;

DROP TABLE IF EXISTS test_int16_boundary;
CREATE TABLE test_int16_boundary (uid Int16) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1;
INSERT INTO test_int16_boundary VALUES (-32768), (-1), (0), (32767);

-- Int16 above max
SELECT count() FROM test_int16_boundary WHERE uid < 32768.0;
SELECT count() FROM test_int16_boundary WHERE uid <= 32768.0;
SELECT count() FROM test_int16_boundary WHERE uid > 32768.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int16_boundary WHERE uid >= 32768.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int16_boundary WHERE uid = 32768.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int16_boundary WHERE uid != 32768.0;

-- Int16 below min
SELECT count() FROM test_int16_boundary WHERE uid < -32769.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int16_boundary WHERE uid <= -32769.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int16_boundary WHERE uid > -32769.0;
SELECT count() FROM test_int16_boundary WHERE uid >= -32769.0;
SELECT count() FROM test_int16_boundary WHERE uid = -32769.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_int16_boundary WHERE uid != -32769.0;


DROP TABLE IF EXISTS test_uint16_boundary;
CREATE TABLE test_uint16_boundary (uid UInt16) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1;
INSERT INTO test_uint16_boundary VALUES (0), (1), (65535);

-- UInt16 above max
SELECT count() FROM test_uint16_boundary WHERE uid < 65536.0;
SELECT count() FROM test_uint16_boundary WHERE uid <= 65536.0;
SELECT count() FROM test_uint16_boundary WHERE uid > 65536.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint16_boundary WHERE uid >= 65536.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint16_boundary WHERE uid = 65536.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint16_boundary WHERE uid != 65536.0;

-- UInt16 below min
SELECT count() FROM test_uint16_boundary WHERE uid < -1.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint16_boundary WHERE uid <= -1.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint16_boundary WHERE uid > -1.0;
SELECT count() FROM test_uint16_boundary WHERE uid >= -1.0;
SELECT count() FROM test_uint16_boundary WHERE uid = -1.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint16_boundary WHERE uid != -1.0;

DROP TABLE IF EXISTS test_uint32_boundary;
CREATE TABLE test_uint32_boundary (uid UInt32) ENGINE = MergeTree ORDER BY uid SETTINGS index_granularity = 1;
INSERT INTO test_uint32_boundary VALUES (0), (1), (4294967295);

-- UInt32 above max
SELECT count() FROM test_uint32_boundary WHERE uid < 4294967296.0;
SELECT count() FROM test_uint32_boundary WHERE uid <= 4294967296.0;
SELECT count() FROM test_uint32_boundary WHERE uid > 4294967296.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint32_boundary WHERE uid >= 4294967296.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint32_boundary WHERE uid = 4294967296.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint32_boundary WHERE uid != 4294967296.0;

-- UInt32 below min
SELECT count() FROM test_uint32_boundary WHERE uid < -1.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint32_boundary WHERE uid <= -1.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint32_boundary WHERE uid > -1.0;
SELECT count() FROM test_uint32_boundary WHERE uid >= -1.0;
SELECT count() FROM test_uint32_boundary WHERE uid = -1.0 SETTINGS force_primary_key = 1, max_rows_to_read = 1;
SELECT count() FROM test_uint32_boundary WHERE uid != -1.0;
