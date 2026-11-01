-- Complementary coverage for arrayDistance after the SIMD multi-target refactor:
-- ensures that the streaming mixed-type kernels (`executeDistanceMixed` and
-- `executeDistanceConstMixed`, which cast per element rather than buffering a
-- whole column) agree bit-for-bit with explicitly cast inputs, across:
--   * const-left / non-const variants for L1 / L2Squared / Linf (L2 and cosine
--     are already covered in 04267_array_distance_mixed_types)
--   * large-array cases under a tight memory limit, for all five SIMD distances.

DROP TABLE IF EXISTS array_distance_simd_coverage;

CREATE TABLE array_distance_simd_coverage
(
    id UInt64,
    u8 Array(UInt8),
    f32 Array(Float32),
    f64 Array(Float64)
)
ENGINE = Memory;

INSERT INTO array_distance_simd_coverage VALUES
    (1, [1, 2, 3, 4], [1.5, 2.5, 3.5, 4.5], [1.25, 2.25, 3.25, 4.25]),
    (2, [5, 8, 13, 21], [4.0, 7.5, 12.5, 20.5], [4.25, 7.25, 12.25, 20.25]),
    (3, [34, 55, 89, 144], [33.5, 54.5, 88.5, 143.5], [33.25, 54.25, 88.25, 143.25]);

-- Section A: const vs non-const for L1 / L2Squared / Linf, both sides, mixed numeric types.
SELECT
    countIf(
        L1Distance([toUInt8(1), 2, 3, 4], f64) = L1Distance(CAST([toUInt8(1), 2, 3, 4], 'Array(Float64)'), f64)
        AND L1Distance(f64, [toUInt8(1), 2, 3, 4]) = L1Distance(f64, CAST([toUInt8(1), 2, 3, 4], 'Array(Float64)'))
        AND L2SquaredDistance([toUInt8(1), 2, 3, 4], f64) = L2SquaredDistance(CAST([toUInt8(1), 2, 3, 4], 'Array(Float64)'), f64)
        AND L2SquaredDistance(f64, [toUInt8(1), 2, 3, 4]) = L2SquaredDistance(f64, CAST([toUInt8(1), 2, 3, 4], 'Array(Float64)'))
        AND LinfDistance([toUInt8(1), 2, 3, 4], f64) = LinfDistance(CAST([toUInt8(1), 2, 3, 4], 'Array(Float64)'), f64)
        AND LinfDistance(f64, [toUInt8(1), 2, 3, 4]) = LinfDistance(f64, CAST([toUInt8(1), 2, 3, 4], 'Array(Float64)'))
    ) = count()
FROM array_distance_simd_coverage;

DROP TABLE array_distance_simd_coverage;

-- Section B: large arrays with mixed numeric types.
-- Two orthogonal signals, intentionally split: a tight memory limit must not
-- catch the correctness check (which materialises a Float64 cast column), and
-- the correctness check must not be relaxed by a generous limit.
DROP TABLE IF EXISTS array_distance_simd_coverage_large;

CREATE TABLE array_distance_simd_coverage_large
(
    id UInt64,
    u8 Array(UInt8),
    f64 Array(Float64)
)
ENGINE = Memory;

INSERT INTO array_distance_simd_coverage_large
SELECT
    number,
    arrayMap(i -> toUInt8(i % 256), range(262144)),
    arrayMap(i -> toFloat64(i % 257), range(262144))
FROM numbers(4);

-- B.1: tight memory limit smoke test for all five SIMD distances. Catches
-- regressions where a distance kernel allocates an internal full-column
-- buffer on the non-const mixed-type path (the very issue fixed by
-- "Remove castData full-column buffer in arrayDistance mixed-type path").
SET max_memory_usage = '6Mi';

SELECT countIf(isFinite(L1Distance(u8, f64))) = 4 FROM array_distance_simd_coverage_large;
SELECT countIf(isFinite(L2Distance(u8, f64))) = 4 FROM array_distance_simd_coverage_large;
SELECT countIf(isFinite(L2SquaredDistance(u8, f64))) = 4 FROM array_distance_simd_coverage_large;
SELECT countIf(isFinite(LinfDistance(u8, f64))) = 4 FROM array_distance_simd_coverage_large;
SELECT countIf(isFinite(cosineDistance(u8, f64))) = 4 FROM array_distance_simd_coverage_large;

-- B.2: numeric correctness on large arrays, comparing against the explicit
-- CAST equivalent. Memory limit is relaxed because the explicit CAST itself
-- materialises an Array(Float64) column.
SET max_memory_usage = '64Mi';

WITH CAST(u8, 'Array(Float64)') AS u8_f64
SELECT countIf(L1Distance(u8, f64) = L1Distance(u8_f64, f64)) = 4
FROM array_distance_simd_coverage_large;

WITH CAST(u8, 'Array(Float64)') AS u8_f64
SELECT countIf(L2Distance(u8, f64) = L2Distance(u8_f64, f64)) = 4
FROM array_distance_simd_coverage_large;

WITH CAST(u8, 'Array(Float64)') AS u8_f64
SELECT countIf(L2SquaredDistance(u8, f64) = L2SquaredDistance(u8_f64, f64)) = 4
FROM array_distance_simd_coverage_large;

WITH CAST(u8, 'Array(Float64)') AS u8_f64
SELECT countIf(LinfDistance(u8, f64) = LinfDistance(u8_f64, f64)) = 4
FROM array_distance_simd_coverage_large;

WITH CAST(u8, 'Array(Float64)') AS u8_f64
SELECT countIf(cosineDistance(u8, f64) = cosineDistance(u8_f64, f64)) = 4
FROM array_distance_simd_coverage_large;

DROP TABLE array_distance_simd_coverage_large;
