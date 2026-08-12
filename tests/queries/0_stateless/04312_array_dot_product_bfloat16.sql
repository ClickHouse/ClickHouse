-- Tests BFloat16 support for arrayDotProduct (widened to Float32 in the auto-vectorized kernel).
-- BFloat16 was previously rejected. Results are checked against an independent Float64 computation
-- within a tolerance, so the reference is stable across architectures (the kernel accumulates in
-- Float32 and may reduce in a different order than the scalar reference).

DROP TABLE IF EXISTS bf16_vectors;
CREATE TABLE bf16_vectors (id UInt64, v Array(BFloat16), f Array(Float32)) ENGINE = Memory;

-- Length 150 is not a multiple of the kernel unroll width (16), so both the unrolled body and the
-- scalar tail loop are exercised. Positive values keep the dot product away from sign cancellation,
-- so relative error reflects accuracy. `f` is a distinct Float32 vector for the mixed-type cases.
INSERT INTO bf16_vectors
SELECT
    number,
    arrayMap(i -> toBFloat16(((number * 7 + i * 13) % 97) / 7.0 + 1.0), range(150)),
    arrayMap(i -> toFloat32(((number * 11 + i * 5) % 83) / 6.0 + 0.5), range(150))
FROM numbers(200);

-- Constant-left SIMD path (dotProductConstBatchImpl): a constant vector against the column.
WITH (SELECT v FROM bf16_vectors WHERE id = 0) AS a
SELECT
    max(abs(arrayDotProduct(a, v) - arraySum(arrayMap((x, y) -> toFloat64(x) * toFloat64(y), a, v)))
        / arraySum(arrayMap((x, y) -> toFloat64(x) * toFloat64(y), a, v))) < 0.001
FROM bf16_vectors;

-- Column-vs-column SIMD path (dotProductBatchImpl): both arguments are the column.
SELECT
    max(abs(arrayDotProduct(v, v) - arraySum(arrayMap(x -> toFloat64(x) * toFloat64(x), v)))
        / arraySum(arrayMap(x -> toFloat64(x) * toFloat64(x), v))) < 0.001
FROM bf16_vectors;

-- Mixed-type scalar path: BFloat16 against another type does NOT take the same-type SIMD kernel, it
-- goes through the scalar accumulator. Both orderings (BF16 x T and T x BF16) must match each other
-- and the Float64 reference, for both column-vs-column and the const-argument path.
WITH (SELECT f FROM bf16_vectors WHERE id = 0) AS af
SELECT
    -- column BFloat16 x column Float32, checked against the Float64 reference
    max(abs(arrayDotProduct(v, f) - arraySum(arrayMap((x, y) -> toFloat64(x) * toFloat64(y), v, f)))
        / arraySum(arrayMap((x, y) -> toFloat64(x) * toFloat64(y), v, f))) < 0.001,
    -- ordering is irrelevant: dotProduct(v, f) == dotProduct(f, v)
    min(arrayDotProduct(v, f) = arrayDotProduct(f, v)),
    -- const Float32 x column BFloat16 (const-argument scalar path), both orderings
    max(abs(arrayDotProduct(af, v) - arraySum(arrayMap((x, y) -> toFloat64(x) * toFloat64(y), af, v)))
        / arraySum(arrayMap((x, y) -> toFloat64(x) * toFloat64(y), af, v))) < 0.001,
    min(arrayDotProduct(af, v) = arrayDotProduct(v, af))
FROM bf16_vectors;

-- The result type is Float32 (matching arrayNorm / arrayDistance for BFloat16 inputs).
SELECT toTypeName(arrayDotProduct(materialize([toBFloat16(1)]), materialize([toBFloat16(1)])));

-- Mixed-type result type (BFloat16 x Float32).
SELECT toTypeName(arrayDotProduct(materialize([toBFloat16(1)]), materialize([toFloat32(1)])));

-- Exact small-integer cases (BFloat16 represents small integers exactly).
SELECT
    arrayDotProduct([toBFloat16(1), toBFloat16(2), toBFloat16(3)], [toBFloat16(4), toBFloat16(5), toBFloat16(6)]),
    arrayDotProduct([toBFloat16(0), toBFloat16(1), toBFloat16(2), toBFloat16(3)], [toBFloat16(0), toBFloat16(1), toBFloat16(2), toBFloat16(3)]);

DROP TABLE bf16_vectors;
