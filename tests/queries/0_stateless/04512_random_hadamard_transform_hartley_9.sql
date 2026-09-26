-- randomHadamardTransform for the 2^N * 9 family (e.g. 1152 = 128 * 9). Order 9 has no +-1 Hadamard
-- matrix (those exist only for orders 1, 2, and multiples of 4), so the exact non-padding small
-- factor is a real orthogonal Discrete Hartley Transform C_9, applied as the Kronecker product
-- H_(2^k) (x) C_9. The output keeps the input dimension instead of padding to the next power of two.

-- Length is preserved (no padding) for the whole 2^k * 9 family, including 1152 = 128 * 9.
SELECT length(randomHadamardTransform(CAST(range(9), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(18), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(36), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(72), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(576), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(1152), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(2304), 'Array(Float32)')));

-- The transform is orthogonal (norm-preserving): ||y||^2 / ||x||^2 == 1.
SELECT round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(9), 'Array(Float32)'), 5)) / arraySum(x -> x * x, CAST(range(9), 'Array(Float32)')) - 1), 4),
       round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 5)) / arraySum(x -> x * x, CAST(range(18), 'Array(Float32)')) - 1), 4),
       round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(1152), 'Array(Float32)'), 7)) / arraySum(x -> x * x, CAST(range(1152), 'Array(Float32)')) - 1), 4);

-- Norm preservation holds for the Float64 compute path too (tighter tolerance).
SELECT round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(1152), 'Array(Float64)'), 7)) / arraySum(x -> x * x, CAST(range(1152), 'Array(Float64)')) - 1), 6);

-- Exact DHT coordinates (rounded): pins the C_9 coefficient convention, the D order applied before
-- the transform, the block-then-cross-block stage order, and the sign stream.
SELECT arrayMap(x -> round(x, 4), randomHadamardTransform(CAST(range(9), 'Array(Float32)')));         -- default seed, blocks = 1
SELECT arrayMap(x -> round(x, 4), randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 42));    -- another seed, blocks = 2

-- Deterministic in the seed; different seeds generally differ.
SELECT randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 42) = randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 42);
SELECT randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 1) = randomHadamardTransform(CAST(range(18), 'Array(Float32)'), 2);

-- output_dims produces a subsampled projection. A genuine truncation of the 2^k * 9 family falls back
-- to the zero-padded power-of-two transform (the exact C_9 rows are not uniform-leverage, so truncating
-- them would be position-biased); an output_dims above the input length still throws.
SELECT length(randomHadamardTransform(CAST(range(1152), 'Array(Float32)'), 7, 500));
SELECT randomHadamardTransform(CAST(range(9), 'Array(Float32)'), 0, 16); -- { serverError ARGUMENT_OUT_OF_BOUND }

-- The result keeps the input's element type.
SELECT toTypeName(randomHadamardTransform(CAST(range(9), 'Array(Float32)'))),
       toTypeName(randomHadamardTransform(CAST(range(9), 'Array(Float64)'))),
       toTypeName(randomHadamardTransform(CAST(range(9), 'Array(BFloat16)')));
