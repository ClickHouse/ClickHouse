-- randomHadamardTransform: deterministic randomized (Walsh-)Hadamard transform of a float vector.

-- Output length is the input length padded to the next power of two.
SELECT length(randomHadamardTransform([1, 2, 3]::Array(Float32))),
       length(randomHadamardTransform(CAST(range(100), 'Array(Float32)')));

-- The full transform is orthogonal (norm-preserving): ||y||^2 == ||x||^2.
SELECT round(abs(arraySum(x -> x * x, randomHadamardTransform([1, 2, 3, 4]::Array(Float32))) - 30), 4);
SELECT round(abs(arraySum(x -> x * x, randomHadamardTransform([1, 2, 3, 4, 5, 6, 7, 8]::Array(Float32), 123)) - 204), 4);

-- Exact values pin the sign stream, the D then H order, and the padding/stage logic.
SELECT randomHadamardTransform([1, 2, 3, 4]::Array(Float32));        -- default seed
SELECT randomHadamardTransform([1, 2, 3, 4]::Array(Float32), 42);    -- another seed
SELECT randomHadamardTransform([1, 2, 3]::Array(Float32));           -- padded length 3 -> 4
SELECT randomHadamardTransform([1, 2, 3, 4]::Array(BFloat16));       -- BFloat16 path

-- Deterministic in the seed; the default seed is 0.
SELECT randomHadamardTransform([1, 2, 3, 4]::Array(Float32), 42) = randomHadamardTransform([1, 2, 3, 4]::Array(Float32), 42);
SELECT randomHadamardTransform([1, 2, 3, 4]::Array(Float32), 1) = randomHadamardTransform([1, 2, 3, 4]::Array(Float32), 2);
SELECT randomHadamardTransform([1, 2, 3, 4]::Array(Float32)) = randomHadamardTransform([1, 2, 3, 4]::Array(Float32), 0);

-- output_dims truncates the result (subsampled randomized Hadamard transform).
SELECT length(randomHadamardTransform(CAST(range(16), 'Array(Float32)'), 7, 5));

-- The result keeps the input's element type.
SELECT toTypeName(randomHadamardTransform([1, 2]::Array(Float32))),
       toTypeName(randomHadamardTransform([1, 2]::Array(Float64))),
       toTypeName(randomHadamardTransform([1, 2]::Array(BFloat16)));

-- An empty input array yields an empty array (output_dims does not apply).
SELECT randomHadamardTransform([]::Array(Float32), 0, 5);

-- Dimensions of the form 2^k * m with m in {12, 20} use an exact Kronecker transform
-- H_(2^k) (x) H_m, so the output keeps the input dimension instead of padding to a power of two.
SELECT length(randomHadamardTransform(CAST(range(12), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(20), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(24), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(768), 'Array(Float32)'))),
       length(randomHadamardTransform(CAST(range(2560), 'Array(Float32)')));

-- The Kronecker transform is orthogonal (norm-preserving): ||y||^2 / ||x||^2 == 1.
SELECT round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(12), 'Array(Float32)'), 5)) / arraySum(x -> x * x, CAST(range(12), 'Array(Float32)')) - 1), 4),
       round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(20), 'Array(Float32)'), 5)) / arraySum(x -> x * x, CAST(range(20), 'Array(Float32)')) - 1), 4),
       round(abs(arraySum(x -> x * x, randomHadamardTransform(CAST(range(768), 'Array(Float32)'), 7)) / arraySum(x -> x * x, CAST(range(768), 'Array(Float32)')) - 1), 4);

-- Exact Kronecker coordinates (rounded): catches a wrong H_m sign convention, D order, or
-- scalar/NEON divergence that would still preserve length and norm.
SELECT arrayMap(x -> round(x, 4), randomHadamardTransform(CAST(range(12), 'Array(Float32)')));

-- output_dims still truncates a Kronecker transform (it must not exceed the input dimension).
SELECT length(randomHadamardTransform(CAST(range(768), 'Array(Float32)'), 7, 500));

-- Errors.
SELECT randomHadamardTransform([1, 2, 3, 4]::Array(Float32), 0, 8); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT randomHadamardTransform(CAST(range(768), 'Array(Float32)'), 0, 800); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT randomHadamardTransform([]::Array(Float32), 0, -1); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT randomHadamardTransform([1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT randomHadamardTransform([1, 2]::Array(Float32), materialize(1)); -- { serverError ILLEGAL_COLUMN }
