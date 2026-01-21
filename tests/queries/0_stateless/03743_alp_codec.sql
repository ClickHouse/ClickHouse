DROP TABLE IF EXISTS base32; CREATE TABLE base32 (i UInt16 CODEC(NONE), f  Float32 CODEC(NONE)) ORDER BY i;
DROP TABLE IF EXISTS base64; CREATE TABLE base64 (i UInt16 CODEC(NONE), f  Float64 CODEC(NONE)) ORDER BY i;
DROP TABLE IF EXISTS alp32; CREATE TABLE alp32 (i UInt16 CODEC(NONE), f  Float32 CODEC(ALP)) ORDER BY i;
DROP TABLE IF EXISTS alp64; CREATE TABLE alp64 (i UInt16 CODEC(NONE), f  Float64 CODEC(ALP)) ORDER BY i;


SELECT 'Test Data integrity (Positive and Exception numbers)';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(toFloat64(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(2000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT number, round(toFloat64(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(2000);
TRUNCATE TABLE base32; INSERT INTO base32 SELECT number, round(toFloat32(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(2000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT number, round(toFloat32(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(2000);
SELECT 'f64 errors:', sum(bin(a.f) <> bin(b.f)) AS errors FROM base64 AS b INNER JOIN alp64 AS a USING i;
SELECT 'f32 errors:', sum(bin(a.f) <> bin(b.f)) AS errors FROM base32 AS b INNER JOIN alp32 AS a USING i;
SELECT table, round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 4) AS compression_ratio
FROM system.parts WHERE database = currentDatabase() AND active AND table IN ('alp32', 'alp64')
GROUP BY table ORDER BY compression_ratio DESC;


SELECT 'Test Data integrity (Positive and Negative numbers)';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(toFloat64(number / 20 + sin(number)), 3) * if(number % 10 == 0, -1.0, 1.0) FROM numbers(2000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT number, round(toFloat64(number / 20 + sin(number)), 3) * if(number % 10 == 0, -1.0, 1.0) FROM numbers(2000);
TRUNCATE TABLE base32; INSERT INTO base32 SELECT number, round(toFloat32(number / 20 + sin(number)), 3) * if(number % 10 == 0, -1.0, 1.0) FROM numbers(2000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT number, round(toFloat32(number / 20 + sin(number)), 3) * if(number % 10 == 0, -1.0, 1.0) FROM numbers(2000);
SELECT 'f64 errors:', sum(bin(a.f) <> bin(b.f)) AS errors FROM base64 AS b INNER JOIN alp64 AS a USING i;
SELECT 'f32 errors:', sum(bin(a.f) <> bin(b.f)) AS errors FROM base32 AS b INNER JOIN alp32 AS a USING i;
SELECT table, round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 4) AS compression_ratio
FROM system.parts WHERE database = currentDatabase() AND active AND table IN ('alp32', 'alp64')
GROUP BY table ORDER BY compression_ratio DESC;


SELECT 'Test Special Float Values (NaN, Inf, Zeros, Subnormals, Boundary)';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number,
    CASE number % 200
        WHEN 2 THEN nan
        WHEN 4 THEN inf
        WHEN 6 THEN -inf
        WHEN 8 THEN -toFloat64(0.0)
        WHEN 10 THEN 4.9406564584124654e-324
        WHEN 12 THEN -4.9406564584124654e-324
        WHEN 14 THEN 2.2250738585072009e-308
        WHEN 16 THEN -2.2250738585072009e-308
        WHEN 18 THEN 2.2250738585072014e-308
        WHEN 20 THEN -2.2250738585072014e-308
        WHEN 22 THEN 1.7976931348623157e308
        WHEN 24 THEN -1.7976931348623157e308
        WHEN 26 THEN 922337203685477478.0
        WHEN 28 THEN -922337203685477478.0
        WHEN 30 THEN 1e18
        WHEN 32 THEN -1e18
        ELSE round(number * 0.001, 3)
END
FROM numbers(2000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT * FROM base64;
TRUNCATE TABLE base32; INSERT INTO base32 SELECT number,
    CASE number % 200
        WHEN 2  THEN toFloat32(nan)
        WHEN 4  THEN toFloat32(inf)
        WHEN 6  THEN toFloat32(-inf)
        WHEN 8  THEN -toFloat32(0.0)
        WHEN 10 THEN toFloat32(1.40129846e-45)
        WHEN 12 THEN toFloat32(-1.40129846e-45)
        WHEN 14 THEN toFloat32(1.17549421e-38)
        WHEN 16 THEN toFloat32(-1.17549421e-38)
        WHEN 18 THEN toFloat32(1.17549435e-38)
        WHEN 20 THEN toFloat32(-1.17549435e-38)
        WHEN 22 THEN toFloat32(3.4028235e38)
        WHEN 24 THEN toFloat32(-3.4028235e38)
        WHEN 26 THEN 922337203685477478.0
        WHEN 28 THEN -922337203685477478.0
        WHEN 30 THEN toFloat32(1e10)
        WHEN 32 THEN toFloat32(-1e10)
        ELSE toFloat32(round(number * 0.001, 3))
END AS f
FROM numbers(2000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT * FROM base32;
SELECT 'f64 errors:', sum(bin(a.f) <> bin(b.f)) AS errors FROM base64 AS b INNER JOIN alp64 AS a USING i;
SELECT 'f32 errors:', sum(bin(a.f) <> bin(b.f)) AS errors FROM base32 AS b INNER JOIN alp32 AS a USING i;
SELECT table, round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 4) AS compression_ratio
FROM system.parts WHERE database = currentDatabase() AND active AND table IN ('alp32', 'alp64')
GROUP BY table ORDER BY compression_ratio DESC;


SELECT 'Compare compression ratio with Gorilla codec (higher is better)';
DROP TABLE IF EXISTS gorilla32; CREATE TABLE gorilla32 (i UInt16 CODEC(NONE), f  Float32 CODEC(Gorilla)) ORDER BY i;
DROP TABLE IF EXISTS gorilla64; CREATE TABLE gorilla64 (i UInt16 CODEC(NONE), f  Float64 CODEC(Gorilla)) ORDER BY i;
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT number, round(toFloat64(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(10000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT number, round(toFloat32(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(10000);
INSERT INTO gorilla64 SELECT number, round(toFloat64(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(10000);
INSERT INTO gorilla32 SELECT number, round(toFloat32(number / 20 + sin(number)), 3) + if(number % 10 == 0, pi(), 0) FROM numbers(10000);
SELECT table, round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 4) AS compression_ratio
FROM system.parts WHERE database = currentDatabase() AND active AND table IN ('alp32', 'alp64', 'gorilla32', 'gorilla64')
GROUP BY table ORDER BY compression_ratio DESC;
DROP TABLE gorilla32;
DROP TABLE gorilla64;


SELECT 'Test different precisions in one data series compression ratio';
TRUNCATE TABLE alp64;
INSERT INTO alp64 SELECT level * 1024 + num AS id, round(toFloat64(num + 1 + sin(num / 10)), level) AS value
FROM (SELECT i1.number AS level, i2.number AS num FROM numbers(8) AS i1 CROSS JOIN numbers(1024) AS i2) ORDER BY id;
SELECT table, round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 4) AS compression_ratio
FROM system.parts WHERE database = currentDatabase() AND active AND table IN ('alp64')
GROUP BY table ORDER BY compression_ratio DESC;


DROP TABLE base32;
DROP TABLE base64;
DROP TABLE alp32;
DROP TABLE alp64;
