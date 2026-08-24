-- randomHadamardTransform: the generalized Discrete Hartley small-factor path, and the exception on
-- full-transform lengths that have no exact form.
--
-- A length 2^k * m whose odd part m has no +-1 Hadamard matrix uses the exact Kronecker transform
-- H_(2^k) (x) C_m with a dense real orthogonal DHT matrix C_m, for ANY odd m up to 64 (order 9 is
-- covered by 04512; this test covers 7, 11, 13, 25, 63, ...). The full transform keeps the input
-- length instead of zero-padding to the next power of two. A full transform of a length whose odd
-- part exceeds 64 has no exact form and is rejected with an exception rather than silently padded.

-- The full transform keeps the input length for the whole generalized family, including embedding
-- dimensions such as 896 = 128 * 7, 1408 = 128 * 11, and 3584 = 512 * 7 (gte-Qwen2-7B-instruct).
SELECT length(randomHadamardTransform(CAST(range(7), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(11), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(13), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(25), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(896), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(1408), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(3584), 'Array(Float32)')));

-- The transform is orthogonal (norm-preserving): ||y||^2 / ||x||^2 == 1.
SELECT round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(7), 'Array(Float32)'), 5)) / arraySum(x -> x * x, CAST(range(7), 'Array(Float32)')) - 1), 4),
       round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(13), 'Array(Float32)'), 5)) / arraySum(x -> x * x, CAST(range(13), 'Array(Float32)')) - 1), 4),
       round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(3584), 'Array(Float32)'), 7)) / arraySum(x -> x * x, CAST(range(3584), 'Array(Float32)')) - 1), 4);

-- Norm preservation holds for the Float64 compute path too (tighter tolerance).
SELECT round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(896), 'Array(Float64)'), 7)) / arraySum(x -> x * x, CAST(range(896), 'Array(Float64)')) - 1), 6);

-- Exact DHT-7 coordinates (rounded): pins the C_m coefficient convention for a generalized order
-- (order 7 has no +-1 Hadamard matrix), the D order applied before the transform, and the sign stream.
SELECT arrayMap(x -> round(x, 4), randomHadamardTransform(CAST(range(7), 'Array(Float32)')));

-- Deterministic in the seed; different seeds generally differ.
SELECT randomHadamardTransform(CAST(range(7), 'Array(Float32)'), 42) = randomHadamardTransform(CAST(range(7), 'Array(Float32)'), 42);
SELECT randomHadamardTransform(CAST(range(7), 'Array(Float32)'), 1) = randomHadamardTransform(CAST(range(7), 'Array(Float32)'), 2);

-- Boundary: odd part 63 (<= max_hartley_block 64) is exact, and so is its power-of-two multiple.
SELECT length(randomHadamardTransform(CAST(range(63), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(126), 'Array(Float32)')));

-- A genuine truncation of a Hartley-family length falls back to the zero-padded power-of-two
-- projection, whose rows have uniform leverage: for length 14 = 2 * 7, every one-hot basis vector
-- projected to output_dims = 2 has the SAME squared norm (1). A single distinct value proves the
-- padded path is used (the exact C_7 prefix would be position-biased).
SELECT DISTINCT round(arraySum(x -> x * x, randomHadamardTransform(arrayMap(i -> toFloat32(i = number), range(14)), 0, 2)), 4) AS v
FROM numbers(14);

-- A full transform of a length whose odd part exceeds 64 has no exact form; instead of zero-padding
-- to a longer power of two (which would silently change the output length) it is rejected.
SELECT randomHadamardTransform(CAST(range(65), 'Array(Float32)'));   -- { serverError BAD_ARGUMENTS }  65 = 65 (odd part 65)
SELECT randomHadamardTransform(CAST(range(130), 'Array(Float32)'));  -- { serverError BAD_ARGUMENTS }  130 = 2 * 65
SELECT randomHadamardTransform(CAST(range(127), 'Array(Float32)'));  -- { serverError BAD_ARGUMENTS }  127 is prime
SELECT randomHadamardTransform(CAST(range(1000), 'Array(Float32)')); -- { serverError BAD_ARGUMENTS }  1000 = 8 * 125

-- Such a length is still accepted as a projection whenever an explicit output_dims is given: that
-- zero-pads to a power of two internally, and the output length is output_dims regardless, so nothing
-- is silently changed. This holds for output_dims below, equal to, and above the input length (up to
-- the padded power of two) -- an explicit output_dims accepts any length, even one that has no exact
-- full transform.
SELECT length(randomHadamardTransform(CAST(range(1000), 'Array(Float32)'), 0, 128)),
       length(randomHadamardTransform(CAST(range(1000), 'Array(Float32)'), 0, 1000)),
       length(randomHadamardTransform(CAST(range(1000), 'Array(Float32)'), 0, 1024)),
       length(randomHadamardTransform(CAST(range(130), 'Array(Float32)'), 0, 64)),
       length(randomHadamardTransform(CAST(range(130), 'Array(Float32)'), 0, 130));

-- output_dims above the padded power of two (here 8 for length 7, whose exact transform length is 7)
-- still throws.
SELECT randomHadamardTransform(CAST(range(7), 'Array(Float32)'), 0, 16); -- { serverError ARGUMENT_OUT_OF_BOUND }
