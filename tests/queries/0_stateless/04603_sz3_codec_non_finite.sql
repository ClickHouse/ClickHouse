-- Tags: no-fasttest, no-random-settings
-- no-fasttest: needs sz3 library
-- no-random-settings: a randomized `max_compress_block_size` that is not a multiple of the float width
-- is rejected by the SZ3 codec at write time

-- Non-finite values (NaN, +-inf) cannot be quantized: the quantizer used to cast
-- `fabs(data - pred) / error_bound` to an integer without checking that the value is representable,
-- which is undefined behavior for NaN and infinities (caught by UBSan in stress tests).
-- Such values must instead be stored losslessly on the unpredictable path, so they round-trip exactly
-- while finite values stay within the error bound.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS tab_sz3_non_finite;

CREATE TABLE tab_sz3_non_finite (
    key    UInt64,
    orig64 Float64,
    orig32 Float32,
    f64_default Float64 CODEC(SZ3),
    f32_default Float32 CODEC(SZ3),
    f64_abs Float64 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01)),
    f32_abs Float32 CODEC(SZ3('ALGO_INTERP', 'ABS', 0.01))
) ENGINE = MergeTree ORDER BY key;

INSERT INTO tab_sz3_non_finite
SELECT
    number,
    v, v, v, v, v, v
FROM
(
    SELECT
        number,
        multiIf(number = 3, nan, number = 500, inf, number = 501, -inf, sin(number * 0.01) * 100) AS v
    FROM numbers(1000)
);

SELECT 'NaN and infinities survive the round trip, finite values stay within the error bound';
-- The last two columns are a positive signal that the ABS columns really stayed on the lossy SZ3 path:
-- a lossless fallback would reproduce every finite value bit-exactly. (The default-configured columns are
-- expected to be stored losslessly here: the default REL error bound is relative to the value range, which
-- is infinite for this data, so SZ3 keeps them bit-exact by design.)
SELECT
    countIf(isNaN(f64_default)) = 1,
    countIf(f64_default = inf) = 1,
    countIf(f64_default = -inf) = 1,
    countIf(isNaN(f32_default)) = 1,
    countIf(f32_default = inf) = 1,
    countIf(f32_default = -inf) = 1,
    countIf(isNaN(f64_abs)) = 1,
    countIf(f64_abs = inf) = 1,
    countIf(f64_abs = -inf) = 1,
    maxIf(abs(orig64 - f64_abs), isFinite(orig64)) <= 0.011,
    maxIf(abs(orig32 - f32_abs), isFinite(orig32)) <= 0.011,
    countIf(f64_abs != orig64 AND isFinite(orig64)) > 0,
    countIf(f32_abs != orig32 AND isFinite(orig32)) > 0
FROM tab_sz3_non_finite;

SELECT 'The same holds after the data is recompressed by a merge';
OPTIMIZE TABLE tab_sz3_non_finite FINAL;
SELECT
    countIf(isNaN(f64_default)) = 1,
    countIf(f64_default = inf) = 1,
    countIf(f64_default = -inf) = 1,
    countIf(isNaN(f32_default)) = 1,
    countIf(f32_default = inf) = 1,
    countIf(f32_default = -inf) = 1,
    countIf(isNaN(f64_abs)) = 1,
    countIf(f64_abs = inf) = 1,
    countIf(f64_abs = -inf) = 1,
    maxIf(abs(orig64 - f64_abs), isFinite(orig64)) <= 0.011,
    maxIf(abs(orig32 - f32_abs), isFinite(orig32)) <= 0.011,
    countIf(f64_abs != orig64 AND isFinite(orig64)) > 0,
    countIf(f32_abs != orig32 AND isFinite(orig32)) > 0
FROM tab_sz3_non_finite;

DROP TABLE tab_sz3_non_finite;
