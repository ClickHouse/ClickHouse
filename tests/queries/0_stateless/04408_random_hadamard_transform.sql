-- randomHadamardTransform: deterministic randomized (Walsh-)Hadamard transform of a float vector.

-- Output length is the input length padded to the next power of two.
SELECT length(randomHadamardTransform([1, 2, 3]::Array(Float32))),
       length(randomHadamardTransform(CAST(range(100), 'Array(Float32)')));

-- The full transform is orthogonal (norm-preserving): ||y||^2 == ||x||^2.
SELECT round(abs(arraySum(x -> x * x, randomHadamardTransform([1, 2, 3, 4]::Array(Float32))) - 30), 4);
SELECT round(abs(arraySum(x -> x * x, randomHadamardTransform([1, 2, 3, 4, 5, 6, 7, 8]::Array(Float32), 123)) - 204), 4);

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

-- Errors.
SELECT randomHadamardTransform([1, 2, 3, 4]::Array(Float32), 0, 8); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT randomHadamardTransform([1, 2, 3]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT randomHadamardTransform([1, 2]::Array(Float32), materialize(1)); -- { serverError ILLEGAL_COLUMN }
