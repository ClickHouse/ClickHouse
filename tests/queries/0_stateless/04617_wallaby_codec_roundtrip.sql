SET enable_wallaby_codec = 1;

DROP TABLE IF EXISTS base32;
DROP TABLE IF EXISTS base64;
DROP TABLE IF EXISTS wallaby32;
DROP TABLE IF EXISTS wallaby64;

CREATE TABLE base32 (i UInt32 CODEC(NONE), f Float32 CODEC(NONE)) ENGINE = MergeTree ORDER BY i;
CREATE TABLE base64 (i UInt32 CODEC(NONE), f Float64 CODEC(NONE)) ENGINE = MergeTree ORDER BY i;
CREATE TABLE wallaby32 (i UInt32 CODEC(NONE), f Float32 CODEC(Wallaby)) ENGINE = MergeTree ORDER BY i;
CREATE TABLE wallaby64 (i UInt32 CODEC(NONE), f Float64 CODEC(Wallaby)) ENGINE = MergeTree ORDER BY i;

SELECT '# Decimal time series (delta-friendly)';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(sin(number / 100) * 1000 + number * 0.01, 2) FROM numbers(30000);
TRUNCATE TABLE wallaby64; INSERT INTO wallaby64 SELECT i, f FROM base64;
TRUNCATE TABLE base32; INSERT INTO base32 SELECT i, toFloat32(f) FROM base64;
TRUNCATE TABLE wallaby32; INSERT INTO wallaby32 SELECT i, f FROM base32;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN wallaby64 AS a USING i;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN wallaby32 AS a USING i;

SELECT '# Decimal series with exceptions (occasional high-precision values)';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, if(number % 97 = 0, exp(number % 43), round(number * 0.25, 2)) FROM numbers(30000);
TRUNCATE TABLE wallaby64; INSERT INTO wallaby64 SELECT i, f FROM base64;
TRUNCATE TABLE base32; INSERT INTO base32 SELECT i, toFloat32(f) FROM base64;
TRUNCATE TABLE wallaby32; INSERT INTO wallaby32 SELECT i, f FROM base32;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN wallaby64 AS a USING i;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN wallaby32 AS a USING i;

SELECT '# Real doubles (XOR path)';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, sin(number / 100) * exp(number % 40) FROM numbers(30000);
TRUNCATE TABLE wallaby64; INSERT INTO wallaby64 SELECT i, f FROM base64;
TRUNCATE TABLE base32; INSERT INTO base32 SELECT i, toFloat32(f) FROM base64;
TRUNCATE TABLE wallaby32; INSERT INTO wallaby32 SELECT i, f FROM base32;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN wallaby64 AS a USING i;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN wallaby32 AS a USING i;

SELECT '# Arbitrary bit patterns, including NaN payloads, infinities, denormals';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, reinterpretAsFloat64(reinterpretAsFixedString(cityHash64(number))) FROM numbers(30000);
TRUNCATE TABLE wallaby64; INSERT INTO wallaby64 SELECT i, f FROM base64;
TRUNCATE TABLE base32; INSERT INTO base32 SELECT number, reinterpretAsFloat32(reinterpretAsFixedString(toUInt32(cityHash64(number) % 4294967296))) FROM numbers(30000);
TRUNCATE TABLE wallaby32; INSERT INTO wallaby32 SELECT i, f FROM base32;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN wallaby64 AS a USING i;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN wallaby32 AS a USING i;

SELECT '# Constant and repeated values, signed zeros';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, multiIf(number < 10000, 42.42, number < 20000, if(number % 2 = 0, 0., -0.), 1e100) FROM numbers(30000);
TRUNCATE TABLE wallaby64; INSERT INTO wallaby64 SELECT i, f FROM base64;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN wallaby64 AS a USING i;

SELECT '# Special values in tiny parts';
TRUNCATE TABLE wallaby64; INSERT INTO wallaby64 VALUES (0, 3.14);
INSERT INTO wallaby64 VALUES (1, nan), (2, inf), (3, -inf), (4, -0.), (5, 1.5e-300);
SELECT i, f FROM wallaby64 ORDER BY i;
TRUNCATE TABLE wallaby32; INSERT INTO wallaby32 VALUES (0, 3.14);
INSERT INTO wallaby32 VALUES (1, nan), (2, inf), (3, -inf), (4, -0.), (5, 1.5e-30);
SELECT i, f FROM wallaby32 ORDER BY i;

SELECT '# Compression is effective on a decimal time series';
DROP TABLE IF EXISTS size_base;
DROP TABLE IF EXISTS size_wallaby;
CREATE TABLE size_base (i UInt32 CODEC(NONE), f Float64 CODEC(NONE)) ENGINE = MergeTree ORDER BY i
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576;
CREATE TABLE size_wallaby (i UInt32 CODEC(NONE), f Float64 CODEC(Wallaby)) ENGINE = MergeTree ORDER BY i
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 65536, max_compress_block_size = 1048576;
INSERT INTO size_base SELECT number, round(sin(number / 1000) * 1000, 2) FROM numbers(100000);
INSERT INTO size_wallaby SELECT i, f FROM size_base;
OPTIMIZE TABLE size_base FINAL;
OPTIMIZE TABLE size_wallaby FINAL;
SELECT (SELECT sum(data_compressed_bytes) FROM system.columns WHERE database = currentDatabase() AND table = 'size_wallaby' AND name = 'f')
     < (SELECT sum(data_compressed_bytes) FROM system.columns WHERE database = currentDatabase() AND table = 'size_base' AND name = 'f') / 4;
DROP TABLE size_base;
DROP TABLE size_wallaby;

SELECT '# Wallaby is not applicable to non-float types and takes no arguments';
CREATE TABLE wallaby_bad (x UInt64 CODEC(Wallaby)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE wallaby_bad (x String CODEC(Wallaby)) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE wallaby_bad (x Float64 CODEC(Wallaby(1))) ENGINE = MergeTree ORDER BY tuple(); -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

DROP TABLE base32;
DROP TABLE base64;
DROP TABLE wallaby32;
DROP TABLE wallaby64;
