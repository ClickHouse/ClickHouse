-- Tests the AVX-512 BFloat16 kernels of arrayDistance (accumulateCombineBF16 / accumulateCombineBF16V4).
-- Results are checked against an independent Float64 computation within a tolerance, so the reference is
-- stable across architectures (the kernels compute in Float32 and may reduce in a different order).

DROP TABLE IF EXISTS bf16_vectors;
CREATE TABLE bf16_vectors (id UInt64, v Array(BFloat16)) ENGINE = Memory;

-- 200 rows of length-80 vectors (80 > 64 exercises the main 4-accumulator loop and the scalar tail).
INSERT INTO bf16_vectors
SELECT number, arrayMap(i -> toBFloat16(((number * 7 + i * 13) % 97) / 7.0 - 7.0), range(80))
FROM numbers(200);

-- SIMD path: the left argument is a constant vector, the right one is the column.
-- Every metric must match the Float64 reference within tolerance (1 = pass).
WITH (SELECT v FROM bf16_vectors WHERE id = 0) AS a
SELECT
    max(abs(L1Distance(a, v)        - arraySum(arrayMap((x, y) -> abs(toFloat64(x) - toFloat64(y)), a, v)))) < 0.01,
    max(abs(L2SquaredDistance(a, v) - arraySum(arrayMap((x, y) -> (toFloat64(x) - toFloat64(y)) * (toFloat64(x) - toFloat64(y)), a, v)))) < 0.1,
    max(abs(L2Distance(a, v)        - sqrt(arraySum(arrayMap((x, y) -> (toFloat64(x) - toFloat64(y)) * (toFloat64(x) - toFloat64(y)), a, v))))) < 0.01,
    max(abs(LinfDistance(a, v)      - arrayMax(arrayMap((x, y) -> abs(toFloat64(x) - toFloat64(y)), a, v)))) < 0.001,
    max(abs(cosineDistance(a, v)    - (1 - arraySum(arrayMap((x, y) -> toFloat64(x) * toFloat64(y), a, v))
        / (sqrt(arraySum(arrayMap(x -> toFloat64(x) * toFloat64(x), a))) * sqrt(arraySum(arrayMap(y -> toFloat64(y) * toFloat64(y), v))))))) < 0.0001
FROM bf16_vectors;

-- The distance of a vector to itself is zero (cosine up to Float32 rounding).
WITH (SELECT v FROM bf16_vectors WHERE id = 5) AS a
SELECT
    L1Distance(a, a) = 0,
    L2Distance(a, a) = 0,
    L2SquaredDistance(a, a) = 0,
    LinfDistance(a, a) = 0,
    cosineDistance(a, a) < 0.0001;

-- A few exact small-integer cases (BFloat16 represents small integers exactly).
SELECT
    L1Distance([toBFloat16(1), toBFloat16(2), toBFloat16(3)], [toBFloat16(0), toBFloat16(0), toBFloat16(0)]),
    L2SquaredDistance([toBFloat16(1), toBFloat16(2), toBFloat16(3)], [toBFloat16(0), toBFloat16(0), toBFloat16(0)]),
    LinfDistance([toBFloat16(1), toBFloat16(2), toBFloat16(3)], [toBFloat16(0), toBFloat16(0), toBFloat16(0)]);

DROP TABLE bf16_vectors;
