SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS base32;
CREATE TABLE base32 (i UInt32 CODEC(NONE), f Float32 CODEC(NONE)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS base64;
CREATE TABLE base64 (i UInt32 CODEC(NONE), f Float64 CODEC(NONE)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp32;
CREATE TABLE alp32 (i UInt32 CODEC(NONE), f Float32 CODEC(ALP)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp64;
CREATE TABLE alp64 (i UInt32 CODEC(NONE), f Float64 CODEC(ALP)) Engine = MergeTree ORDER BY i;


SELECT '# Positive Numbers Test';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(3000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
TRUNCATE TABLE base32; INSERT INTO base32 SELECT i, toFloat32(f) FROM base64;;
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT i, f FROM base32;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN alp32 AS a USING i;


SELECT '# Positive and Negative Numbers Test';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) * if(number % 5 == 0, -1.0, 1.0) FROM numbers(3000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
TRUNCATE TABLE base32; INSERT INTO base32 SELECT i, toFloat32(f) FROM base64;;
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT i, f FROM base32;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN alp32 AS a USING i;


SELECT '# Negative Numbers Test';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, -round(number * 0.1 + cos(number), 2) FROM numbers(3000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
TRUNCATE TABLE base32; INSERT INTO base32 SELECT i, toFloat32(f) FROM base64;;
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT i, f FROM base32;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN alp32 AS a USING i;


SELECT '# All Same Values Test';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, 3.14 FROM numbers(3000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
TRUNCATE TABLE base32; INSERT INTO base32 SELECT i, toFloat32(f) FROM base64;
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT i, f FROM base32;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN alp32 AS a USING i;


SELECT '# Mix Rational and Irrational Numbers Test';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, if(number % 10 = 1, number * 0.1 + cos(number), round(number * 0.1 + cos(number), 2)) FROM numbers(3000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
TRUNCATE TABLE base32; INSERT INTO base32 SELECT i, toFloat32(f) FROM base64;
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT i, f FROM base32;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN alp32 AS a USING i;


DROP TABLE base32;
DROP TABLE base64;
DROP TABLE alp32;
DROP TABLE alp64;
