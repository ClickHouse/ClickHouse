-- Tests the AVX-512 integer kernel of arrayDistance (accumulateCombineIntegerV4), which widens integer
-- inputs to Float64. Several integer types are covered. Integer distances are exact in Float64, so for
-- L1/L2Squared/Linf the SIMD kernel must match the scalar reference exactly; L2/Cosine involve sqrt and
-- division, so they are checked within a tolerance. Either way the reference is stable across architectures.

DROP TABLE IF EXISTS int_vectors;
CREATE TABLE int_vectors (id UInt64, v Array(Int64)) ENGINE = Memory;

-- length-80 vectors (80 > 64 exercises the main 4-accumulator loop and the scalar tail).
INSERT INTO int_vectors
SELECT number, arrayMap(i -> toInt64((number * 7 + i * 13) % 200 - 100), range(80))
FROM numbers(200);

-- SIMD path: the left argument is a constant vector, the right one is the column.
WITH (SELECT v FROM int_vectors WHERE id = 0) AS a
SELECT
    -- exact for the metrics whose accumulation stays within Float64's exact integer range
    max(L1Distance(a, v)        = arraySum(arrayMap((x, y) -> abs(toFloat64(x) - toFloat64(y)), a, v))),
    max(L2SquaredDistance(a, v) = arraySum(arrayMap((x, y) -> (toFloat64(x) - toFloat64(y)) * (toFloat64(x) - toFloat64(y)), a, v))),
    max(LinfDistance(a, v)      = arrayMax(arrayMap((x, y) -> abs(toFloat64(x) - toFloat64(y)), a, v))),
    -- tolerance for the metrics with a sqrt / division
    max(abs(L2Distance(a, v)     - sqrt(arraySum(arrayMap((x, y) -> (toFloat64(x) - toFloat64(y)) * (toFloat64(x) - toFloat64(y)), a, v))))) < 0.0001,
    max(abs(cosineDistance(a, v) - (1 - arraySum(arrayMap((x, y) -> toFloat64(x) * toFloat64(y), a, v))
        / (sqrt(arraySum(arrayMap(x -> toFloat64(x) * toFloat64(x), a))) * sqrt(arraySum(arrayMap(y -> toFloat64(y) * toFloat64(y), v))))))) < 0.0001
FROM int_vectors;

DROP TABLE int_vectors;

-- Same checks for a few more integer types (UInt8, Int32, UInt32), via the SIMD const-argument path.
DROP TABLE IF EXISTS i32;
CREATE TABLE i32 (id UInt64, v Array(Int32)) ENGINE = Memory;
INSERT INTO i32 SELECT number, arrayMap(i -> toInt32((number * 11 + i * 5) % 4000 - 2000), range(80)) FROM numbers(200);
WITH (SELECT v FROM i32 WHERE id = 0) AS a
SELECT
    max(L1Distance(a, v) = arraySum(arrayMap((x, y) -> abs(toFloat64(x) - toFloat64(y)), a, v))),
    max(LinfDistance(a, v) = arrayMax(arrayMap((x, y) -> abs(toFloat64(x) - toFloat64(y)), a, v)))
FROM i32;
DROP TABLE i32;

DROP TABLE IF EXISTS u8;
CREATE TABLE u8 (id UInt64, v Array(UInt8)) ENGINE = Memory;
INSERT INTO u8 SELECT number, arrayMap(i -> toUInt8((number * 11 + i * 5) % 256), range(80)) FROM numbers(200);
WITH (SELECT v FROM u8 WHERE id = 0) AS a
SELECT
    max(L1Distance(a, v) = arraySum(arrayMap((x, y) -> abs(toFloat64(x) - toFloat64(y)), a, v))),
    max(L2SquaredDistance(a, v) = arraySum(arrayMap((x, y) -> (toFloat64(x) - toFloat64(y)) * (toFloat64(x) - toFloat64(y)), a, v)))
FROM u8;
DROP TABLE u8;

-- Exact small cases.
SELECT
    L1Distance([toInt32(1), toInt32(2), toInt32(3)], [toInt32(4), toInt32(6), toInt32(8)]),
    L2SquaredDistance([toInt32(1), toInt32(2), toInt32(3)], [toInt32(4), toInt32(6), toInt32(8)]),
    LinfDistance([toInt32(1), toInt32(2), toInt32(3)], [toInt32(4), toInt32(6), toInt32(8)]);
