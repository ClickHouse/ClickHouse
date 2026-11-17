SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS base32;
CREATE TABLE base32 (i UInt32 CODEC(NONE), f Float32 CODEC(NONE)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS base64;
CREATE TABLE base64 (i UInt32 CODEC(NONE), f Float64 CODEC(NONE)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp32;
CREATE TABLE alp32 (i UInt32 CODEC(NONE), f Float32 CODEC(ALP)) Engine = MergeTree ORDER BY i;

DROP TABLE IF EXISTS alp64;
CREATE TABLE alp64 (i UInt32 CODEC(NONE), f Float64 CODEC(ALP)) Engine = MergeTree ORDER BY i;


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
        -- Near clamp bounds (UPPER = 9223372036854773760.0)
        WHEN 18 THEN 9223372036854773760.0  -- exactly at upper bound
        WHEN 19 THEN -9223372036854773760.0 -- exactly at lower bound
        WHEN 20 THEN 9223372036854773761.0   -- just above upper bound
        WHEN 21 THEN -9223372036854773761.0  -- just below lower bound
        WHEN 22 THEN 9223372036854773000.0   -- near upper bound
        WHEN 23 THEN -9223372036854773000.0  -- near lower bound
        -- Large values beyond clamp
        WHEN 24 THEN 1e19
        WHEN 25 THEN -1e19
        WHEN 26 THEN 1e20
        WHEN 27 THEN -1e20
        -- Regular decimal values
        ELSE round(number * 0.1 + cos(number), 2)
    END
FROM numbers(4000);
TRUNCATE TABLE alp64; INSERT INTO alp64 SELECT i, f FROM base64;
-- Compare bit patterns, but treat -0.0 and +0.0 as equivalent (sparse serialization may normalize -0.0 to +0.0).
-- Reason: https://github.com/ClickHouse/ClickHouse/issues/98637.
-- We can return normal version once this issue is fixed.
SELECT count(), sum(
    reinterpretAsUInt64(a.f) <> reinterpretAsUInt64(b.f)
    AND NOT (reinterpretAsUInt64(a.f) IN (0, 0x8000000000000000) AND reinterpretAsUInt64(b.f) IN (0, 0x8000000000000000))
) FROM base64 AS b INNER JOIN alp64 AS a USING i;

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
        -- Clamp bounds (UPPER = 9223371487098961920.0f)
        WHEN 18 THEN 9223371487098961920.0  -- exactly at upper bound
        WHEN 19 THEN -9223371487098961920.0 -- exactly at lower bound
        -- Large values (testing precision)
        WHEN 20 THEN toFloat32(1e10)
        WHEN 21 THEN toFloat32(-1e10)
        WHEN 22 THEN toFloat32(1e20)
        WHEN 23 THEN toFloat32(-1e20)
        WHEN 24 THEN toFloat32(1e-10)
        WHEN 25 THEN toFloat32(-1e-10)
        -- Regular decimal values
        ELSE toFloat32(round(number * 0.1 + cos(number), 2))
    END
FROM numbers(4000);
TRUNCATE TABLE alp32; INSERT INTO alp32 SELECT * FROM base32;
SELECT count(), sum(
    reinterpretAsUInt32(a.f) <> reinterpretAsUInt32(b.f)
    AND NOT (reinterpretAsUInt32(a.f) IN (0, 0x80000000) AND reinterpretAsUInt32(b.f) IN (0, 0x80000000))
) FROM base32 AS b INNER JOIN alp32 AS a USING i;


DROP TABLE base32;
DROP TABLE base64;
DROP TABLE alp32;
DROP TABLE alp64;
