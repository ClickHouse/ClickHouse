DROP TABLE IF EXISTS base32;
DROP TABLE IF EXISTS base64;
DROP TABLE IF EXISTS alp32;
DROP TABLE IF EXISTS alp64;
DROP TABLE IF EXISTS gorilla32;
DROP TABLE IF EXISTS gorilla64;

CREATE TABLE base32 (i UInt16 CODEC(NONE), f  Float32 CODEC(NONE)) ORDER BY i;
CREATE TABLE base64 (i UInt16 CODEC(NONE), f  Float64 CODEC(NONE)) ORDER BY i;
CREATE TABLE alp32 (i UInt16 CODEC(NONE), f  Float32 CODEC(ALP)) ORDER BY i;
CREATE TABLE alp64 (i UInt16 CODEC(NONE), f  Float64 CODEC(ALP)) ORDER BY i;
CREATE TABLE gorilla32 (i UInt16 CODEC(NONE), f  Float32 CODEC(Gorilla)) ORDER BY i;
CREATE TABLE gorilla64 (i UInt16 CODEC(NONE), f  Float64 CODEC(Gorilla)) ORDER BY i;

SELECT 'Test Data integrity';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(toFloat64(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(2000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT number, round(toFloat64(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(2000);
TRUNCATE TABLE base32; INSERT INTO base32 SELECT number, round(toFloat32(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(2000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT number, round(toFloat32(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(2000);
SELECT SUM(a.f <> b.f) AS errors FROM base64 AS b INNER JOIN alp64 AS a USING i;
SELECT SUM(a.f <> b.f) AS errors FROM base32 AS b INNER JOIN alp32 AS a USING i;

SELECT 'Compare compression ratio with Gorilla codec (higher is better)';
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT number, round(toFloat64(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(10000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT number, round(toFloat32(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(10000);
TRUNCATE TABLE gorilla64; INSERT INTO gorilla64 SELECT number, round(toFloat64(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(10000);
TRUNCATE TABLE gorilla32; INSERT INTO gorilla32 SELECT number, round(toFloat32(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(10000);
OPTIMIZE TABLE gorilla32 FINAL; OPTIMIZE TABLE gorilla64 FINAL; OPTIMIZE TABLE alp32 FINAL; OPTIMIZE TABLE alp64 FINAL;
SELECT table,
    SUM(data_compressed_bytes) AS compressed_bytes,
    SUM(data_uncompressed_bytes) AS uncompressed_bytes,
    ROUND(SUM(data_uncompressed_bytes) / SUM(data_compressed_bytes), 4) AS compression_ratio
FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('alp32', 'alp64', 'gorilla32', 'gorilla64')
GROUP BY table
ORDER BY compression_ratio DESC;

SELECT 'Test incompressible data compression ratio';
TRUNCATE TABLE base32; INSERT INTO base32 SELECT number, toFloat32(922337203685477478 + number * pi()) FROM numbers(10000);
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, toFloat64(922337203685477478 + number * pi()) FROM numbers(10000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT number, toFloat32(922337203685477478 + number * pi()) FROM numbers(10000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT number, toFloat64(922337203685477478 + number * pi()) FROM numbers(10000);
OPTIMIZE TABLE alp32 FINAL; OPTIMIZE TABLE alp64 FINAL; OPTIMIZE TABLE base32 FINAL; OPTIMIZE TABLE base64 FINAL;
SELECT SUM(a.f <> b.f) AS errors FROM base32 AS b INNER JOIN alp32 AS a USING i;
SELECT SUM(a.f <> b.f) AS errors FROM base64 AS b INNER JOIN alp64 AS a USING i;
SELECT table,
       SUM(data_compressed_bytes) AS compressed_bytes,
       SUM(data_uncompressed_bytes) AS uncompressed_bytes,
       ROUND(SUM(data_uncompressed_bytes) / SUM(data_compressed_bytes), 4) AS compression_ratio
FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('alp32', 'alp64', 'base32', 'base64')
GROUP BY table
ORDER BY compression_ratio DESC;

SELECT 'Test different precisions in one data series compression ratio';
TRUNCATE TABLE alp64;
INSERT INTO alp64 SELECT level * 1024 + num AS id, round(toFloat64(num + 1 + sin(num / 10)), level) AS value
FROM (SELECT i1.number AS level, i2.number AS num FROM numbers(8) AS i1 CROSS JOIN numbers(1024) AS i2) ORDER BY id;
OPTIMIZE TABLE alp64 FINAL;
SELECT table,
       SUM(data_compressed_bytes) AS compressed_bytes,
       SUM(data_uncompressed_bytes) AS uncompressed_bytes,
       ROUND(SUM(data_uncompressed_bytes) / SUM(data_compressed_bytes), 4) AS compression_ratio
FROM system.parts
WHERE database = currentDatabase() AND active AND table IN ('alp64')
GROUP BY table
ORDER BY compression_ratio DESC;

DROP TABLE base32;
DROP TABLE base64;
DROP TABLE alp32;
DROP TABLE alp64;
DROP TABLE gorilla32;
DROP TABLE gorilla64;
