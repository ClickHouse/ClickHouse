-- Test basic output dimensions (downscaling)
SELECT 'Normal downscale dim';
SELECT length(randomProjectionNormal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42));
SELECT 'Orthogonal downscale dim';
SELECT length(randomProjectionOrthogonal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42));
SELECT 'Sparse downscale dim';
SELECT length(randomProjectionSparse([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42));
SELECT 'Hadamard downscale dim';
SELECT length(randomProjectionHadamard([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42));

-- Test basic output dimensions (upscaling)
SELECT 'Normal upscale dim';
SELECT length(randomProjectionNormal([1.0, 2.0]::Array(Float32), 4, 42));
SELECT 'Orthogonal upscale dim';
SELECT length(randomProjectionOrthogonal([1.0, 2.0]::Array(Float32), 4, 42));
SELECT 'Sparse upscale dim';
SELECT length(randomProjectionSparse([1.0, 2.0]::Array(Float32), 4, 42));
SELECT 'Hadamard upscale dim';
SELECT length(randomProjectionHadamard([1.0, 2.0]::Array(Float32), 4, 42));

-- Test determinism: same seed produces same result
SELECT 'Determinism Normal';
SELECT randomProjectionNormal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42)
     = randomProjectionNormal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42);
SELECT 'Determinism Orthogonal';
SELECT randomProjectionOrthogonal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42)
     = randomProjectionOrthogonal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42);
SELECT 'Determinism Sparse';
SELECT randomProjectionSparse([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42)
     = randomProjectionSparse([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42);
SELECT 'Determinism Hadamard';
SELECT randomProjectionHadamard([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42)
     = randomProjectionHadamard([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42);

-- Test different seeds produce different results (extremely unlikely to be equal)
SELECT 'Different seeds Normal';
SELECT randomProjectionNormal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42)
    != randomProjectionNormal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 99);
SELECT 'Different seeds Orthogonal';
SELECT randomProjectionOrthogonal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42)
    != randomProjectionOrthogonal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 99);
SELECT 'Different seeds Sparse';
SELECT randomProjectionSparse([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42)
    != randomProjectionSparse([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 99);
SELECT 'Different seeds Hadamard';
SELECT randomProjectionHadamard([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42)
    != randomProjectionHadamard([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 99);

-- Test Float64 support for all methods
SELECT 'Float64 Normal';
SELECT length(randomProjectionNormal([1.0, 2.0, 3.0, 4.0]::Array(Float64), 2, 42));
SELECT toTypeName(randomProjectionNormal([1.0, 2.0, 3.0, 4.0]::Array(Float64), 2, 42));
SELECT toTypeName(randomProjectionNormal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 2, 42));
SELECT 'Float64 Orthogonal';
SELECT toTypeName(randomProjectionOrthogonal([1.0, 2.0, 3.0, 4.0]::Array(Float64), 2, 42));
SELECT 'Float64 Sparse';
SELECT toTypeName(randomProjectionSparse([1.0, 2.0, 3.0, 4.0]::Array(Float64), 2, 42));
SELECT 'Float64 Hadamard';
SELECT toTypeName(randomProjectionHadamard([1.0, 2.0, 3.0, 4.0]::Array(Float64), 2, 42));

-- Test multi-row scan via subquery
SELECT 'Table scan';
SELECT id, length(randomProjectionNormal(embedding, 2, 42))
FROM (SELECT 1 AS id, [1.0, 2.0, 3.0, 4.0]::Array(Float32) AS embedding
      UNION ALL SELECT 2, [5.0, 6.0, 7.0, 8.0]
      UNION ALL SELECT 3, [9.0, 10.0, 11.0, 12.0])
ORDER BY id;

-- Test identity-like dimension (same dim)
SELECT 'Same dim';
SELECT length(randomProjectionNormal([1.0, 2.0, 3.0, 4.0]::Array(Float32), 4, 42));

-- Test distance preservation for all methods (downscaling 8 -> 4)
SELECT 'Distance preservation Normal';
SELECT abs(
    L2Distance(
        randomProjectionNormal([1,0,0,0,0,0,0,0]::Array(Float32), 4, 123),
        randomProjectionNormal([0,0,0,0,0,0,0,1]::Array(Float32), 4, 123)
    ) - L2Distance([1,0,0,0,0,0,0,0]::Array(Float32), [0,0,0,0,0,0,0,1]::Array(Float32))
) < 2.0;
SELECT 'Distance preservation Orthogonal';
SELECT abs(
    L2Distance(
        randomProjectionOrthogonal([1,0,0,0,0,0,0,0]::Array(Float32), 4, 123),
        randomProjectionOrthogonal([0,0,0,0,0,0,0,1]::Array(Float32), 4, 123)
    ) - L2Distance([1,0,0,0,0,0,0,0]::Array(Float32), [0,0,0,0,0,0,0,1]::Array(Float32))
) < 2.0;
SELECT 'Distance preservation Sparse';
SELECT abs(
    L2Distance(
        randomProjectionSparse([1,0,0,0,0,0,0,0]::Array(Float32), 4, 123),
        randomProjectionSparse([0,0,0,0,0,0,0,1]::Array(Float32), 4, 123)
    ) - L2Distance([1,0,0,0,0,0,0,0]::Array(Float32), [0,0,0,0,0,0,0,1]::Array(Float32))
) < 2.0;
SELECT 'Distance preservation Hadamard';
SELECT abs(
    L2Distance(
        randomProjectionHadamard([1,0,0,0,0,0,0,0]::Array(Float32), 4, 123),
        randomProjectionHadamard([0,0,0,0,0,0,0,1]::Array(Float32), 4, 123)
    ) - L2Distance([1,0,0,0,0,0,0,0]::Array(Float32), [0,0,0,0,0,0,0,1]::Array(Float32))
) < 2.0;

-- Test multi-row batch for non-Normal methods
SELECT 'Batch Orthogonal';
SELECT length(randomProjectionOrthogonal(embedding, 2, 42))
FROM (SELECT [1.0, 2.0, 3.0, 4.0]::Array(Float32) AS embedding
      UNION ALL SELECT [5.0, 6.0, 7.0, 8.0]);
SELECT 'Batch Sparse';
SELECT length(randomProjectionSparse(embedding, 2, 42))
FROM (SELECT [1.0, 2.0, 3.0, 4.0]::Array(Float32) AS embedding
      UNION ALL SELECT [5.0, 6.0, 7.0, 8.0]);
SELECT 'Batch Hadamard';
SELECT length(randomProjectionHadamard(embedding, 2, 42))
FROM (SELECT [1.0, 2.0, 3.0, 4.0]::Array(Float32) AS embedding
      UNION ALL SELECT [5.0, 6.0, 7.0, 8.0]);

-- Test larger dimensions (exercises SIMD main loops, d=64 > AVX512 width)
SELECT 'Large dim Normal';
SELECT length(randomProjectionNormal(arrayMap(x -> toFloat32(x), range(64)), 16, 42));
SELECT 'Large dim Hadamard';
SELECT length(randomProjectionHadamard(arrayMap(x -> toFloat32(x), range(64)), 16, 42));

-- Test error: non-const target_dim
SELECT randomProjectionNormal([1.0, 2.0]::Array(Float32), number, 42) FROM numbers(1); -- { serverError ILLEGAL_COLUMN }

-- Test error: non-const seed
SELECT randomProjectionNormal([1.0, 2.0]::Array(Float32), 2, number) FROM numbers(1); -- { serverError ILLEGAL_COLUMN }

-- Test error: non-array input
SELECT randomProjectionNormal(42, 2, 42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Test error: wrong array element type
SELECT randomProjectionNormal([1, 2, 3]::Array(UInt8), 2, 42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Test error: target_dim = 0
SELECT randomProjectionNormal([1.0, 2.0]::Array(Float32), 0, 42); -- { serverError BAD_ARGUMENTS }

-- Test error: mismatched array dimensions in batch
SELECT randomProjectionNormal(embedding, 2, 42)
FROM (SELECT [1.0, 2.0, 3.0, 4.0]::Array(Float32) AS embedding
      UNION ALL SELECT [1.0, 2.0, 3.0]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- Test Hadamard with non-power-of-2 input dimension
SELECT 'Hadamard non-power-of-2';
SELECT length(randomProjectionHadamard([1.0, 2.0, 3.0, 4.0, 5.0]::Array(Float32), 3, 42));
