-- Truncation of the 2^k * 9 (order-9 Hartley) family of randomHadamardTransform.
--
-- The exact non-padding small factor C_9 is orthogonal, so the FULL transform is norm-preserving, but
-- its rows do NOT have the uniform leverage of a +-1 Hadamard matrix. A truncated prefix of C_9 rows is
-- therefore a position-biased projection rather than a valid Johnson-Lindenstrauss / SRHT one. So a
-- genuine truncation (output_dims < length) of the 2^k * 9 family falls back to the zero-padded
-- power-of-two transform, whose flat +-1 rows keep that property. The full transform still uses the
-- exact C_9 path, and an output_dims above the input length still throws.

-- Uniform leverage of the truncated projection: for length 18 (= 2 * 9), every one-hot basis vector,
-- projected to output_dims = 2, has the SAME squared norm (1). The exact C_9 prefix would instead give
-- position-dependent values in the 0.5 .. 1.5 range, so a single distinct value proves the padded path
-- is used.
SELECT DISTINCT round(arraySum(x -> x * x, randomHadamardTransform(arrayMap(i -> toFloat32(i = number), range(18)), 0, 2)), 4) AS v
FROM numbers(18);

-- The full transform keeps the exact C_9 path (output length == input length, no padding), whether
-- output_dims is omitted or set explicitly to the input length.
SELECT length(randomHadamardTransform(CAST(range(18), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 0, 18)),
       length(randomHadamardTransform(CAST(range(1152), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(1152), 'Array(Float32)'), 0, 1152));

-- output_dims == length is the full exact transform, identical to omitting output_dims.
SELECT randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 7, 18) = randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 7);

-- A genuine truncation returns the requested number of coordinates (via the padded path).
SELECT length(randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 0, 5)),
       length(randomHadamardTransform(CAST(range(1152), 'Array(Float32)'), 7, 500));

-- The truncated projection is norm-preserving in expectation (padded SRHT): averaged over lanes, a
-- one-hot input keeps unit squared norm.
SELECT round(avg(arraySum(x -> x * x, randomHadamardTransform(arrayMap(i -> toFloat32(i = number), range(1152)), 0, 256))), 4)
FROM numbers(64);

-- An output_dims above the input length still throws (the exact path is not padded away for it).
SELECT randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 0, 32); -- { serverError ARGUMENT_OUT_OF_BOUND }
