SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS base32;
CREATE TABLE base32 (i UInt32 CODEC(NONE), f Float32 CODEC(NONE)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS base64;
CREATE TABLE base64 (i UInt32 CODEC(NONE), f Float64 CODEC(NONE)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp32;
CREATE TABLE alp32 (i UInt32 CODEC(NONE), f Float32 CODEC(ALP)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp64;
CREATE TABLE alp64 (i UInt32 CODEC(NONE), f Float64 CODEC(ALP)) Engine = MergeTree ORDER BY i;


SELECT '# Block Size Test (Float64)';
TRUNCATE TABLE base64; TRUNCATE TABLE alp64;
SELECT 'Size 0', count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;

TRUNCATE TABLE base64; INSERT INTO base64 SELECT 0, 3.14;
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT 'Size 1', count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;

TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(2);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT 'Size 2', count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;

TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(1023);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT 'Size 1023', count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;

TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(1024);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT 'Size 1024', count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;

TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(1025);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT 'Size 1025', count(), sum(bin(a.f) <> bin(b.f))FROM base64 AS b INNER JOIN alp64 AS a USING i;

TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(2047);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT 'Size 2047', count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;

TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(2048);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT 'Size 2048', count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;

TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(2049);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT 'Size 2049', count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;

SELECT '# Block Size Test (Float32)';
TRUNCATE TABLE base32; INSERT INTO base32 SELECT number, toFloat32(round(number * 0.1 + cos(number), 2)) FROM numbers(1023);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT i, f FROM base32;
SELECT 'Size 1023', count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN alp32 AS a USING i;

TRUNCATE TABLE base32; INSERT INTO base32 SELECT number, toFloat32(round(number * 0.1 + cos(number), 2)) FROM numbers(1025);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT i, f FROM base32;
SELECT 'Size 1025', count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN alp32 AS a USING i;


SELECT '# All Non-Rational Floats Test';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, sin(number) + cos(number) * log(toFloat64(number + 1)) FROM numbers(1000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT 'Irrational', count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;

TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, if(number % 2 = 0, nan, inf) FROM numbers(1000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT 'NaN/Infinite', count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;


SELECT '# Mixed Per-Blocks Parameters Test';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number,
    CASE(toUInt32(number / 1024))
        WHEN 0 THEN round(number * 0.1 + cos(number), 1)
        WHEN 1 THEN round(number * 0.1 + cos(number), 2)
        WHEN 2 THEN round(number * 0.1 + cos(number), 4)
        ELSE sin(number) * cos(number) * log(toFloat64(number + 1))
    END
FROM numbers(4096);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;


DROP TABLE base32;
DROP TABLE base64;
DROP TABLE alp32;
DROP TABLE alp64;
