DROP TABLE IF EXISTS base32;
CREATE TABLE base32 (i UInt32 CODEC(NONE), f Float32 CODEC(NONE)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS base64;
CREATE TABLE base64 (i UInt32 CODEC(NONE), f Float64 CODEC(NONE)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp32;
CREATE TABLE alp32 (i UInt32 CODEC(NONE), f Float32 CODEC(ALP)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp64;
CREATE TABLE alp64 (i UInt32 CODEC(NONE), f Float64 CODEC(ALP)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp64_granule2048;
CREATE TABLE alp64_granule2048 (i UInt64 CODEC(NONE), f Float64 CODEC(ALP)) Engine = MergeTree ORDER BY i SETTINGS index_granularity = 2048;


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


SELECT '# Special Values Test (NaN, Inf, Zeros, Subnormals, Clamp Bounds)';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number,
    CASE number % 50
        -- Multiple NaN patterns
        WHEN 0 THEN nan
        WHEN 1 THEN -nan
        WHEN 2 THEN reinterpretAsFloat64(0x7FF0000000000001)  -- signaling NaN
        WHEN 3 THEN reinterpretAsFloat64(0x7FF8000000000001)  -- quiet NaN
        -- Infinities
        WHEN 4 THEN inf
        WHEN 5 THEN -inf
        -- Zero variants
        WHEN 6 THEN toFloat64(0.0)
        WHEN 7 THEN -toFloat64(0.0)
        WHEN 8 THEN 1e-324 * 0.1   -- underflow to +0.0
        WHEN 9 THEN -1e-324 * 0.1  -- underflow to -0.0
        -- Subnormals (below 2.2250738585072014e-308)
        WHEN 10 THEN 4.9406564584124654e-324  -- smallest subnormal
        WHEN 11 THEN -4.9406564584124654e-324
        WHEN 12 THEN 2.2250738585072009e-308  -- largest subnormal
        WHEN 13 THEN -2.2250738585072009e-308
        -- Smallest normals
        WHEN 14 THEN 2.2250738585072014e-308
        WHEN 15 THEN -2.2250738585072014e-308
        -- Largest finite values
        WHEN 16 THEN 1.7976931348623157e308
        WHEN 17 THEN -1.7976931348623157e308
        -- Near clamp bounds (UPPER = 922337203685477478.0)
        WHEN 18 THEN 922337203685477478.0   -- exactly at upper bound
        WHEN 19 THEN -922337203685477478.0  -- exactly at lower bound
        WHEN 20 THEN 922337203685477479.0   -- just above upper bound
        WHEN 21 THEN -922337203685477479.0  -- just below lower bound
        WHEN 22 THEN 922337203685477400.0   -- near upper bound
        WHEN 23 THEN -922337203685477400.0
        -- Large values beyond clamp
        WHEN 24 THEN 1e18
        WHEN 25 THEN -1e18
        WHEN 26 THEN 1e19
        WHEN 27 THEN -1e19
        -- Regular decimal values
        ELSE round(number * 0.1 + cos(number), 2)
    END
FROM numbers(4000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base64 AS b INNER JOIN alp64 AS a USING i;

TRUNCATE TABLE base32; INSERT INTO base32 SELECT number,
    CASE number % 50
        -- Multiple NaN patterns
        WHEN 0 THEN toFloat32(nan)
        WHEN 1 THEN toFloat32(-nan)
        WHEN 2 THEN reinterpretAsFloat32(0x7F800001)  -- signaling NaN
        WHEN 3 THEN reinterpretAsFloat32(0x7FC00001)  -- quiet NaN
        -- Infinities
        WHEN 4 THEN toFloat32(inf)
        WHEN 5 THEN toFloat32(-inf)
        -- Zero variants
        WHEN 6 THEN toFloat32(0.0)
        WHEN 7 THEN -toFloat32(0.0)
        WHEN 8 THEN toFloat32(1e-45) * toFloat32(0.001)  -- underflow
        WHEN 9 THEN toFloat32(-1e-45) * toFloat32(0.001)
        -- Subnormals (below 1.17549435e-38)
        WHEN 10 THEN toFloat32(1.40129846e-45)   -- smallest subnormal
        WHEN 11 THEN toFloat32(-1.40129846e-45)
        WHEN 12 THEN toFloat32(1.17549421e-38)   -- largest subnormal
        WHEN 13 THEN toFloat32(-1.17549421e-38)
        -- Smallest normals
        WHEN 14 THEN toFloat32(1.17549435e-38)
        WHEN 15 THEN toFloat32(-1.17549435e-38)
        -- Largest finite values
        WHEN 16 THEN toFloat32(3.4028235e38)
        WHEN 17 THEN toFloat32(-3.4028235e38)
        -- Large values (testing precision)
        WHEN 18 THEN toFloat32(1e10)
        WHEN 19 THEN toFloat32(-1e10)
        WHEN 20 THEN toFloat32(1e20)
        WHEN 21 THEN toFloat32(-1e20)
        WHEN 22 THEN toFloat32(1e-10)
        WHEN 23 THEN toFloat32(-1e-10)
        -- Regular decimal values
        ELSE toFloat32(round(number * 0.1 + cos(number), 2))
    END
FROM numbers(4000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT * FROM base32;
SELECT count(), sum(bin(a.f) <> bin(b.f)) FROM base32 AS b INNER JOIN alp32 AS a USING i;


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


SELECT '# Aggregations Test';
TRUNCATE TABLE base64; INSERT INTO base64 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(5000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
SELECT count(), abs(sum(a.f) - sum(b.f)) < 0.0001, abs(avg(a.f) - avg(b.f)) < 0.0001, min(a.f) = min(b.f), max(a.f) = max(b.f)
FROM base64 AS b INNER JOIN alp64 AS a USING i
WHERE a.i BETWEEN 1500 AND 3000;

TRUNCATE TABLE base32; INSERT INTO base32 SELECT number, toFloat32(round(number * 0.1 + cos(number), 2)) FROM numbers(5000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT i, f FROM base32;
SELECT count(), abs(sum(a.f) - sum(b.f)) < 0.0001, abs(avg(a.f) - avg(b.f)) < 0.0001, min(a.f) = min(b.f), max(a.f) = max(b.f)
FROM base32 AS b INNER JOIN alp32 AS a USING i
WHERE a.i BETWEEN 1500 AND 3000;


SELECT '# Granule Test';
TRUNCATE TABLE alp64_granule2048;
INSERT INTO alp64_granule2048 SELECT number, round(number * 0.1 + cos(number), 2) FROM numbers(6000);
OPTIMIZE TABLE alp64_granule2048 FINAL;

SELECT 'First Granule', count(), sum(bin(f) <> bin(round(i * 0.1 + cos(i), 2))) FROM alp64_granule2048 WHERE i < 2048;
SELECT 'Middle granules', count(), sum(bin(f) <> bin(round(i * 0.1 + cos(i), 2))) FROM alp64_granule2048 WHERE i >= 2048 AND i < 4096;
SELECT 'Last granule', count(), sum(bin(f) <> bin(round(i * 0.1 + cos(i), 2))) FROM alp64_granule2048 WHERE i >= 4096;
SELECT 'Sparse read', count(), sum(bin(f) <> bin(round(i * 0.1 + cos(i), 2))) FROM alp64_granule2048 WHERE i % 1500 = 0;


DROP TABLE base32;
DROP TABLE base64;
DROP TABLE alp32;
DROP TABLE alp64;
DROP TABLE alp64_granule2048;
