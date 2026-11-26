-- Regression: a Dynamic reference-vector argument of the transposed distance functions makes the result Dynamic.
-- DistanceTransposedPartialReadsPass rewrote the call and cast the reference vector to a concrete Array, so the
-- rewritten result was Float64 while the original was Dynamic. The pass then threw a logical error during analysis
-- (found by the AST fuzzer). Fixed in #109405, which skips the optimization for a Variant or Dynamic reference
-- vector. This test guards both branches of that skip: the result type and values must be identical whether or not
-- the optimization fires. The Variant branch is exercised for dotProductTransposed, which 04493 does not cover.

SET enable_analyzer = 1;
SET allow_experimental_variant_type = 1;
SET use_variant_as_common_type = 1;

DROP TABLE IF EXISTS qbit_plain;
CREATE TABLE qbit_plain (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO qbit_plain VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [8, 7, 6, 5, 4, 3, 2, 1]);

SELECT 'Dynamic reference -> Dynamic result, optimization on';
WITH CAST(arrayMap(x -> toFloat32(x), range(8)), 'Dynamic(max_types=8)') AS ref
SELECT DISTINCT toTypeName(dotProductTransposed(vec, ref, 3)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;
WITH CAST(arrayMap(x -> toFloat32(x), range(8)), 'Dynamic(max_types=8)') AS ref
SELECT DISTINCT toTypeName(L2DistanceTransposed(vec, ref, 3)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;
WITH CAST(arrayMap(x -> toFloat32(x), range(8)), 'Dynamic(max_types=8)') AS ref
SELECT DISTINCT toTypeName(cosineDistanceTransposed(vec, ref, 3)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT 'Same result type with the optimization off';
WITH CAST(arrayMap(x -> toFloat32(x), range(8)), 'Dynamic(max_types=8)') AS ref
SELECT DISTINCT toTypeName(dotProductTransposed(vec, ref, 3)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 0;

SELECT 'Values are identical with the optimization on and off (full precision), all three functions';
WITH CAST(arrayMap(x -> toFloat32(x), range(8)), 'Dynamic(max_types=8)') AS ref
SELECT id,
    round(dotProductTransposed(vec, ref, 8)::Float64, 4),
    round(L2DistanceTransposed(vec, ref, 8)::Float64, 4),
    round(cosineDistanceTransposed(vec, ref, 8)::Float64, 4)
FROM qbit_plain ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 1;
WITH CAST(arrayMap(x -> toFloat32(x), range(8)), 'Dynamic(max_types=8)') AS ref
SELECT id,
    round(dotProductTransposed(vec, ref, 8)::Float64, 4),
    round(L2DistanceTransposed(vec, ref, 8)::Float64, 4),
    round(cosineDistanceTransposed(vec, ref, 8)::Float64, 4)
FROM qbit_plain ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 0;

SELECT 'A plain (non-Dynamic) array reference still uses the optimization: Float64 result';
WITH arrayMap(x -> toFloat32(x), range(8)) AS ref
SELECT DISTINCT toTypeName(dotProductTransposed(vec, ref, 3)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT 'Original fuzzer shape must not throw a logical error during analysis';
SELECT DISTINCT round(dotProductTransposed(vec, CAST(range(8), 'Dynamic(max_types=8)'), 3)::Float64, 4) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;

-- Variant branch of the isVariant(ref_vec_type) || isDynamic(ref_vec_type) skip. A Variant reference vector makes the
-- original result Nullable; the rewritten result type must stay identical whether or not the optimization fires.
SELECT 'Variant reference -> Nullable result, optimization on';
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT DISTINCT toTypeName(dotProductTransposed(vec, ref, 3)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT DISTINCT toTypeName(L2DistanceTransposed(vec, ref, 3)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT DISTINCT toTypeName(cosineDistanceTransposed(vec, ref, 3)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT 'Same Variant result type with the optimization off';
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT DISTINCT toTypeName(dotProductTransposed(vec, ref, 3)) FROM qbit_plain SETTINGS optimize_qbit_distance_function_reads = 0;

SELECT 'Variant reference values are identical with the optimization on and off, all three functions';
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT id,
    round(dotProductTransposed(vec, ref, 8)::Float64, 4),
    round(L2DistanceTransposed(vec, ref, 8)::Float64, 4),
    round(cosineDistanceTransposed(vec, ref, 8)::Float64, 4)
FROM qbit_plain ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 1;
WITH CAST([toFloat32(0), 1, 2, 3, 4, 5, 6, 7] AS Variant(Array(Float32), UInt8)) AS ref
SELECT id,
    round(dotProductTransposed(vec, ref, 8)::Float64, 4),
    round(L2DistanceTransposed(vec, ref, 8)::Float64, 4),
    round(cosineDistanceTransposed(vec, ref, 8)::Float64, 4)
FROM qbit_plain ORDER BY id SETTINGS optimize_qbit_distance_function_reads = 0;

DROP TABLE qbit_plain;
