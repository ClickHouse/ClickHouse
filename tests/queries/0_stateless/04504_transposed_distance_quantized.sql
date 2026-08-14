-- Quantized transposed distance functions: cosineDistanceTransposedQuantized / L2DistanceTransposedQuantized /
-- dotProductTransposedQuantized. They operate on a QBit(Int8) of quantizeBFloat16ToInt8 Lloyd-Max codes, dequantizing
-- each code to its reconstruction level on the fly, and compute the distance against the reference vector. A Float reference is
-- the query, compared directly and cast to Float32 (the reconstruction precision of the dequantized codes, so a Float64 query is
-- narrowed to Float32); an Array(Int8) reference is itself dequantized, always at full 8-bit precision (`p` truncates only the
-- stored QBit), so the comparison is symmetric quantized-vs-quantized only at p = 8.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS qbit_q;
CREATE TABLE qbit_q (id UInt32, codes Array(Int8), vec QBit(Int8, 8)) ENGINE = Memory;

-- Store the raw codes alongside the QBit so the test can self-check against a manual dequantize + regular distance.
INSERT INTO qbit_q
SELECT id, codes, codes::QBit(Int8, 8)
FROM
(
    SELECT 1 AS id, arrayMap(x -> quantizeBFloat16ToInt8(x), [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(BFloat16)) AS codes
    UNION ALL
    SELECT 2 AS id, arrayMap(x -> quantizeBFloat16ToInt8(x), [-0.30, 0.70, -0.10, 0.40, -0.60, 0.20, -1.00, 0.80]::Array(BFloat16)) AS codes
    UNION ALL
    SELECT 3 AS id, arrayMap(x -> quantizeBFloat16ToInt8(x), [0.42, -0.11, 0.88, -0.05, 0.15, -0.33, 0.60, -0.77]::Array(BFloat16)) AS codes
)
ORDER BY id;

SELECT 'At precision 8 the result matches a manual dequantize + regular distance (asymmetric distance computation)';
-- `..._close` must be 1 for every row: the on-the-fly dequantization reconstructs exactly toFloat32(dequantizeInt8ToBFloat16(code)).
WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(Float32) AS ref,
     arrayMap(c -> toFloat32(dequantizeInt8ToBFloat16(c)), codes) AS deq
SELECT
    id,
    abs(cosineDistanceTransposedQuantized(vec, ref, 8) - cosineDistance(deq, ref)) < 1e-4 AS cosine_close,
    abs(L2DistanceTransposedQuantized(vec, ref, 8) - L2Distance(deq, ref)) < 1e-4 AS l2_close,
    abs(dotProductTransposedQuantized(vec, ref, 8) - dotProduct(deq, ref)) < 1e-4 AS dot_close
FROM qbit_q
ORDER BY id;

SELECT 'An Array(Int8) reference is dequantized like the QBit (symmetric quantized-vs-quantized distance)';
-- Passing the quantizeBFloat16ToInt8 codes of the query as an Array(Int8) reference dequantizes both sides at full precision, so
-- the result matches the regular distance on toFloat32(dequantizeInt8ToBFloat16(...)) of both operands. `..._close` must be 1.
WITH arrayMap(x -> quantizeBFloat16ToInt8(x), [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(BFloat16)) AS ref_codes,
     arrayMap(c -> toFloat32(dequantizeInt8ToBFloat16(c)), ref_codes) AS deq_ref,
     arrayMap(c -> toFloat32(dequantizeInt8ToBFloat16(c)), codes) AS deq
SELECT
    id,
    abs(cosineDistanceTransposedQuantized(vec, ref_codes, 8) - cosineDistance(deq, deq_ref)) < 1e-4 AS cosine_close,
    abs(L2DistanceTransposedQuantized(vec, ref_codes, 8) - L2Distance(deq, deq_ref)) < 1e-4 AS l2_close,
    abs(dotProductTransposedQuantized(vec, ref_codes, 8) - dotProduct(deq, deq_ref)) < 1e-4 AS dot_close
FROM qbit_q
ORDER BY id;

SELECT 'An Array(Int8) reference gives identical results with the partial-reads pass on or off (both blocks must be identical)';
WITH arrayMap(x -> quantizeBFloat16ToInt8(x), [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(BFloat16)) AS ref_codes
SELECT id, round(L2DistanceTransposedQuantized(vec, ref_codes, 6), 2) AS d FROM qbit_q ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 0;
WITH arrayMap(x -> quantizeBFloat16ToInt8(x), [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(BFloat16)) AS ref_codes
SELECT id, round(L2DistanceTransposedQuantized(vec, ref_codes, 6), 2) AS d FROM qbit_q ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT 'An Array(Nullable(Int8)) reference gives identical results with the partial-reads pass on or off (regression, no divergence)';
-- An Array cannot be wrapped in Nullable (Nullable(Array(...)) is not a constructible type), so the only nullable Int8 array reference
-- is Array(Nullable(Int8)). Its element type is Nullable(Int8), not the exact Int8 that the quantized functions dequantize, so both the
-- optimizer pass and the ordinary (unoptimized) path treat it as a full-precision Float reference and compare its codes as literal
-- Float32 values rather than dequantizing them. The point of this regression is only that the two settings must not diverge: the pass
-- reads the same raw query-tree type the ordinary path sees after stripping wrappers, so neither ever reaches the Int8 branch. The two
-- blocks below must be identical.
WITH arrayMap(x -> quantizeBFloat16ToInt8(x), [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(BFloat16))::Array(Nullable(Int8)) AS ref
SELECT id, round(L2DistanceTransposedQuantized(vec, ref, 8), 2) AS d FROM qbit_q ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 0;
WITH arrayMap(x -> quantizeBFloat16ToInt8(x), [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(BFloat16))::Array(Nullable(Int8)) AS ref
SELECT id, round(L2DistanceTransposedQuantized(vec, ref, 8), 2) AS d FROM qbit_q ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT 'At p < 8 the Array(Int8) reference is still reconstructed at full 8-bit precision (every *_matches must be 1, pass on or off)';
-- `p` truncates only the stored QBit; the Array(Int8) reference is dequantized at row 8 of the LUT regardless of `p`. Its result must
-- therefore be bit-identical to the identical query passed as its full-precision Float32 reconstruction toFloat32(dequantizeInt8ToBFloat16(...)),
-- for any `p`. This locks the p < 8 contract: if the reference dequant ever started honouring `p` (using row `p`), the equalities below
-- would break for p = 4. Checked with the partial-reads pass off and on.
WITH arrayMap(x -> quantizeBFloat16ToInt8(x), [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(BFloat16)) AS ref_codes,
     arrayMap(c -> toFloat32(dequantizeInt8ToBFloat16(c)), ref_codes)::Array(Float32) AS ref_full
SELECT
    id,
    cosineDistanceTransposedQuantized(vec, ref_codes, 4) = cosineDistanceTransposedQuantized(vec, ref_full, 4) AS cosine_matches,
    L2DistanceTransposedQuantized(vec, ref_codes, 4) = L2DistanceTransposedQuantized(vec, ref_full, 4) AS l2_matches,
    dotProductTransposedQuantized(vec, ref_codes, 4) = dotProductTransposedQuantized(vec, ref_full, 4) AS dot_matches
FROM qbit_q
ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 0;
WITH arrayMap(x -> quantizeBFloat16ToInt8(x), [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(BFloat16)) AS ref_codes,
     arrayMap(c -> toFloat32(dequantizeInt8ToBFloat16(c)), ref_codes)::Array(Float32) AS ref_full
SELECT
    id,
    cosineDistanceTransposedQuantized(vec, ref_codes, 4) = cosineDistanceTransposedQuantized(vec, ref_full, 4) AS cosine_matches,
    L2DistanceTransposedQuantized(vec, ref_codes, 4) = L2DistanceTransposedQuantized(vec, ref_full, 4) AS l2_matches,
    dotProductTransposedQuantized(vec, ref_codes, 4) = dotProductTransposedQuantized(vec, ref_full, 4) AS dot_matches
FROM qbit_q
ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT 'A non-Float32 Float reference is accepted and computed at the Float32 reconstruction precision';
-- A Float reference is cast to Float32 (the reconstruction precision of the dequantized codes), exactly as the non-quantized
-- transposed functions cast the reference to the QBit element type. A Float32 query widened to Float64 round-trips exactly, so a
-- Float64 reference must give a result bit-identical to the Float32 one (every `*_matches` must be 1). A BFloat16 query widens to
-- Float32 exactly, so it matches a manual dequantize + regular distance on toFloat32 of the BFloat16 query (every `*_close` must be 1).
WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(Float32) AS ref
SELECT
    id,
    cosineDistanceTransposedQuantized(vec, ref::Array(Float64), 8) = cosineDistanceTransposedQuantized(vec, ref, 8) AS cosine_matches,
    L2DistanceTransposedQuantized(vec, ref::Array(Float64), 8) = L2DistanceTransposedQuantized(vec, ref, 8) AS l2_matches,
    dotProductTransposedQuantized(vec, ref::Array(Float64), 8) = dotProductTransposedQuantized(vec, ref, 8) AS dot_matches
FROM qbit_q
ORDER BY id;
WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(BFloat16) AS ref_bf16,
     arrayMap(x -> toFloat32(x), ref_bf16) AS ref_f32,
     arrayMap(c -> toFloat32(dequantizeInt8ToBFloat16(c)), codes) AS deq
SELECT
    id,
    abs(cosineDistanceTransposedQuantized(vec, ref_bf16, 8) - cosineDistance(deq, ref_f32)) < 1e-4 AS cosine_close,
    abs(L2DistanceTransposedQuantized(vec, ref_bf16, 8) - L2Distance(deq, ref_f32)) < 1e-4 AS l2_close,
    abs(dotProductTransposedQuantized(vec, ref_bf16, 8) - dotProduct(deq, ref_f32)) < 1e-4 AS dot_close
FROM qbit_q
ORDER BY id;

SELECT 'A Float64 reference gives identical results with the partial-reads pass on or off (both blocks must be identical)';
WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(Float64) AS ref
SELECT id, round(L2DistanceTransposedQuantized(vec, ref, 6), 2) AS d FROM qbit_q ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 0;
WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(Float64) AS ref
SELECT id, round(L2DistanceTransposedQuantized(vec, ref, 6), 2) AS d FROM qbit_q ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT 'Concrete rounded distances at precision 8';
-- Rounded to 2 decimals so SimSIMD NEON vs AVX low-bit differences do not make the reference architecture-dependent.
WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(Float32) AS ref
SELECT
    id,
    round(cosineDistanceTransposedQuantized(vec, ref, 8), 2) AS cosine,
    round(L2DistanceTransposedQuantized(vec, ref, 8), 2) AS l2,
    round(dotProductTransposedQuantized(vec, ref, 8), 2) AS dot
FROM qbit_q
ORDER BY id;

SELECT 'The optimize_qbit_distance_function_reads partial-reads pass does not change the result (both blocks must be identical)';
WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(Float32) AS ref
SELECT id, round(cosineDistanceTransposedQuantized(vec, ref, 6), 2) AS d FROM qbit_q ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 0;
WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(Float32) AS ref
SELECT id, round(cosineDistanceTransposedQuantized(vec, ref, 6), 2) AS d FROM qbit_q ORDER BY id
SETTINGS optimize_qbit_distance_function_reads = 1;

SELECT 'Lower precision reconstructs a coarser embedded quantizer (still stable, less accurate)';
WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(Float32) AS ref
SELECT id, round(cosineDistanceTransposedQuantized(vec, ref, 1), 2) AS p1, round(cosineDistanceTransposedQuantized(vec, ref, 4), 2) AS p4
FROM qbit_q
ORDER BY id;

DROP TABLE qbit_q;


SELECT 'Strided QBit (Matryoshka) with used_dims';
DROP TABLE IF EXISTS qbit_strided;
CREATE TABLE qbit_strided (id UInt32, codes Array(Int8), vec QBit(Int8, 16, 8)) ENGINE = Memory;
INSERT INTO qbit_strided
SELECT id, codes, codes::QBit(Int8, 16, 8)
FROM
(
    SELECT 1 AS id, arrayMap(x -> quantizeBFloat16ToInt8(toBFloat16(x)), arrayMap(i -> (i - 8) / 10, range(16))) AS codes
    UNION ALL
    SELECT 2 AS id, arrayMap(x -> quantizeBFloat16ToInt8(toBFloat16(x)), arrayMap(i -> (8 - i) / 10, range(16))) AS codes
)
ORDER BY id;

-- Read only the first 8 dimensions (one stride group).
WITH arrayMap(i -> (i - 4) / 10, range(8))::Array(Float32) AS ref,
     arrayMap(c -> toFloat32(dequantizeInt8ToBFloat16(c)), arraySlice(codes, 1, 8)) AS deq
SELECT id, abs(L2DistanceTransposedQuantized(vec, ref, 8, 8) - L2Distance(deq, ref)) < 1e-4 AS l2_close
FROM qbit_strided
ORDER BY id;

-- Read all 16 dimensions.
WITH arrayMap(i -> (i - 8) / 10, range(16))::Array(Float32) AS ref,
     arrayMap(c -> toFloat32(dequantizeInt8ToBFloat16(c)), codes) AS deq
SELECT id, abs(L2DistanceTransposedQuantized(vec, ref, 8, 16) - L2Distance(deq, ref)) < 1e-4 AS l2_close
FROM qbit_strided
ORDER BY id;

-- An Array(Int8) reference for the strided form (read the first 8 dimensions), dequantized like the QBit on both sides.
WITH arrayMap(x -> quantizeBFloat16ToInt8(toBFloat16(x)), arrayMap(i -> (i - 4) / 10, range(8))) AS ref_codes,
     arrayMap(c -> toFloat32(dequantizeInt8ToBFloat16(c)), ref_codes) AS deq_ref,
     arrayMap(c -> toFloat32(dequantizeInt8ToBFloat16(c)), arraySlice(codes, 1, 8)) AS deq
SELECT id, abs(L2DistanceTransposedQuantized(vec, ref_codes, 8, 8) - L2Distance(deq, deq_ref)) < 1e-4 AS l2_close
FROM qbit_strided
ORDER BY id;

DROP TABLE qbit_strided;


SELECT 'Type checks';
-- The quantized functions require a QBit(Int8).
DROP TABLE IF EXISTS qbit_f32;
CREATE TABLE qbit_f32 (vec QBit(Float32, 2)) ENGINE = Memory;
INSERT INTO qbit_f32 VALUES ([0.1, 0.2]);
SELECT cosineDistanceTransposedQuantized(vec, [0.1, 0.2]::Array(Float32), 8) FROM qbit_f32; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT cosineDistanceTransposedQuantized(vec, [0.1, 0.2]::Array(Float32), 8) FROM qbit_f32 SETTINGS optimize_qbit_distance_function_reads = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
DROP TABLE qbit_f32;


SELECT 'A Dynamic or Variant reference vector is left unoptimized and still evaluates (regression, no logical error)';
-- A Variant/Dynamic reference vector makes the overload resolver evaluate the function per alternative, so its result is a
-- Variant/Dynamic and it cannot be cast to Array. DistanceTransposedPartialReadsPass must leave such a call untouched instead of
-- throwing a logical error; the value must equal the plain Array(Float32) distance (0.02 for this row), with the pass on or off.
DROP TABLE IF EXISTS qbit_dyn;
CREATE TABLE qbit_dyn (vec QBit(Int8, 8)) ENGINE = Memory;
INSERT INTO qbit_dyn SELECT arrayMap(x -> quantizeBFloat16ToInt8(x), [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(BFloat16))::QBit(Int8, 8);

WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(Float32) AS ref
SELECT
    round(L2DistanceTransposedQuantized(vec, ref::Dynamic, 8)::Float64, 2) AS dyn,
    round(L2DistanceTransposedQuantized(vec, ref::Variant(Array(Float32)), 8)::Float64, 2) AS var
FROM qbit_dyn SETTINGS optimize_qbit_distance_function_reads = 1;
WITH [0.10, -0.50, 0.30, -0.20, 0.05, -0.90, 1.20, -1.50]::Array(Float32) AS ref
SELECT
    round(L2DistanceTransposedQuantized(vec, ref::Dynamic, 8)::Float64, 2) AS dyn,
    round(L2DistanceTransposedQuantized(vec, ref::Variant(Array(Float32)), 8)::Float64, 2) AS var
FROM qbit_dyn SETTINGS optimize_qbit_distance_function_reads = 0;
DROP TABLE qbit_dyn;


SELECT 'A hand-written quantized internal call with a reference vector that is neither Float32 nor Int8 is rejected cleanly';
-- The undocumented internal calling convention (FixedString bit planes, then the size, then the reference vector) is only ever
-- generated with an Array(Float32) query (read as ColumnVector<Float32>) or a quantized Array(Int8) query (dequantized from
-- ColumnVector<Int8>). A hand-written internal call with any other reference element type must be rejected instead of reinterpreting
-- its memory: parseInternalArguments declines it and the call falls through to the user-facing path, which reports that the first
-- argument must be a QBit.
SELECT L2DistanceTransposedQuantized('a'::FixedString(1), 8::UInt64, [1, 2, 3, 4, 5, 6, 7, 8]::Array(BFloat16)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT L2DistanceTransposedQuantized('a'::FixedString(1), 8::UInt64, [1, 2, 3, 4, 5, 6, 7, 8]::Array(Float64)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
