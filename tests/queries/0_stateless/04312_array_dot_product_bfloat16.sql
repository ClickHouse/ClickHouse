-- Tests BFloat16 support for arrayDotProduct (widened to Float32 in the auto-vectorized kernel).
-- BFloat16 was previously rejected. Results are checked against an independent Float64 computation
-- within a tolerance, so the reference is stable across architectures (the kernel accumulates in
-- Float32 and may reduce in a different order than the scalar reference).

DROP TABLE IF EXISTS bf16_vectors;
CREATE TABLE bf16_vectors (id UInt64, v Array(BFloat16)) ENGINE = Memory;

-- 200 rows of length-80 vectors (80 > the unroll width exercises the main loop and the scalar tail).
-- Positive values keep the dot product away from sign cancellation, so relative error reflects accuracy.
INSERT INTO bf16_vectors
SELECT number, arrayMap(i -> toBFloat16(((number * 7 + i * 13) % 97) / 7.0 + 1.0), range(80))
FROM numbers(200);

-- SIMD path: the left argument is a constant vector, the right one is the column.
WITH (SELECT v FROM bf16_vectors WHERE id = 0) AS a
SELECT
    max(abs(arrayDotProduct(a, v) - arraySum(arrayMap((x, y) -> toFloat64(x) * toFloat64(y), a, v)))
        / arraySum(arrayMap((x, y) -> toFloat64(x) * toFloat64(y), a, v))) < 0.001
FROM bf16_vectors;

-- The result type is Float32 (matching arrayNorm / arrayDistance for BFloat16 inputs).
SELECT toTypeName(arrayDotProduct(materialize([toBFloat16(1)]), materialize([toBFloat16(1)])));

-- Exact small-integer cases (BFloat16 represents small integers exactly).
SELECT
    arrayDotProduct([toBFloat16(1), toBFloat16(2), toBFloat16(3)], [toBFloat16(4), toBFloat16(5), toBFloat16(6)]),
    arrayDotProduct([toBFloat16(0), toBFloat16(1), toBFloat16(2), toBFloat16(3)], [toBFloat16(0), toBFloat16(1), toBFloat16(2), toBFloat16(3)]);

DROP TABLE bf16_vectors;
