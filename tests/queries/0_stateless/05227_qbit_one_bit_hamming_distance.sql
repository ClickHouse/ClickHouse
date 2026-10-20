-- At precision 1 only the sign bit plane of a QBit is read, so every stored element is known as a sign s and reconstructed as
-- s * c with a fixed magnitude c (2.0 for a float QBit, 64 for a raw Int8 QBit, the Lloyd-Max prefix centroid sqrt(2 / pi) for
-- the ...Quantized functions). The transposed distance functions compute this without untransposing the plane:
--   * by default the reference vector keeps its full precision (asymmetric distance): the signed sum of +-y over the plane
--     bytes is taken from a 256-entry table per byte, and the result equals the plain distance between the reconstructed
--     vector and the reference;
--   * with `qbit_one_bit_symmetric_distance = 1` the reference vector is reduced to its signs as well and the result is derived
--     from the Hamming distance h between the two sign vectors (XOR + popcount + lookup table), so it equals the plain
--     distance between the two sign vectors of magnitude c: L2 = 2 * c * sqrt(h), cosine = 2 * h / n, dot = c^2 * (n - 2 * h).
-- Both are cross-checked below against the plain distance functions on the explicit vectors.

DROP TABLE IF EXISTS qbit_one_bit;
CREATE TABLE qbit_one_bit
(
    id UInt32,
    bf QBit(BFloat16, 12),
    f32 QBit(Float32, 12),
    f64 QBit(Float64, 12),
    i8 QBit(Int8, 12)
) ENGINE = MergeTree ORDER BY id;

-- 12 dimensions: not a multiple of 8, so the sign plane has padding bits. The sign pattern of a row is given by the bits of
-- its id, while the magnitudes vary per dimension and must not influence the result.
INSERT INTO qbit_one_bit
SELECT number,
       arrayMap(i -> toBFloat16((i + 1) * 0.25 * if(bitTest(number, i), -1, 1)), range(12)),
       arrayMap(i -> toFloat32((i + 1) * 0.25 * if(bitTest(number, i), -1, 1)), range(12)),
       arrayMap(i -> toFloat64((i + 1) * 0.25 * if(bitTest(number, i), -1, 1)), range(12)),
       arrayMap(i -> toInt8((i + 1) * 5 * if(bitTest(number, i), -1, 1)), range(12))
FROM numbers(0, 4096, 337);

SELECT '-- Default (asymmetric): float QBits reconstructed to +-2.0 against the full-precision reference';
WITH
    [3.5, -1, 0.001, -0.75, 2, -2, 0, 100, -0.5, 7, -7, 0.003]::Array(Float32) AS ref,
    arrayMap(x -> if(x < 0, -2., 2.), CAST(f32, 'Array(Float32)')) AS recon
SELECT id,
       round(L2DistanceTransposed(bf, ref, 1), 4) AS bf_l2,
       round(L2DistanceTransposed(f32, ref, 1), 4) AS f32_l2,
       round(L2DistanceTransposed(f64, ref, 1), 4) AS f64_l2,
       round(L2Distance(recon, ref), 4) AS expected_l2,
       round(cosineDistanceTransposed(f32, ref, 1), 4) AS f32_cos,
       round(cosineDistanceTransposed(f64, ref, 1), 4) AS f64_cos,
       round(cosineDistance(recon, ref), 4) AS expected_cos,
       round(dotProductTransposed(bf, ref, 1), 4) AS bf_dot,
       round(dotProductTransposed(f32, ref, 1), 4) AS f32_dot,
       round(dotProductTransposed(f64, ref, 1), 4) AS f64_dot,
       round(dotProduct(recon, ref), 4) AS expected_dot
FROM qbit_one_bit ORDER BY id;

SELECT '-- Default (asymmetric): Int8 QBit reconstructed to +-64 against the full-precision reference';
WITH
    [3, -1, 1, -75, 2, -2, 0, 100, -50, 7, -7, 3]::Array(Int8) AS ref,
    arrayMap(x -> if(x < 0, -64, 64), CAST(i8, 'Array(Int8)')) AS recon
SELECT id,
       round(L2DistanceTransposed(i8, ref, 1), 4) AS i8_l2,
       round(L2Distance(recon, ref), 4) AS expected_l2,
       round(cosineDistanceTransposed(i8, ref, 1), 4) AS i8_cos,
       round(cosineDistance(recon, ref), 4) AS expected_cos,
       dotProductTransposed(i8, ref, 1) AS i8_dot,
       dotProduct(recon, ref) AS expected_dot
FROM qbit_one_bit ORDER BY id;

SELECT '-- Default (asymmetric): non-constant reference, one per row';
WITH
    arrayMap(i -> toFloat32(if(bitTest(id + 1, i), -1, 1) * (i + 1)), range(12)) AS ref,
    arrayMap(x -> if(x < 0, -2., 2.), CAST(f32, 'Array(Float32)')) AS recon
SELECT id,
       round(L2DistanceTransposed(f32, ref, 1), 4) AS l2,
       round(L2Distance(recon, ref), 4) AS expected_l2,
       round(dotProductTransposed(f64, ref, 1), 4) AS dot,
       round(dotProduct(recon, ref), 4) AS expected_dot
FROM qbit_one_bit ORDER BY id;

SELECT '-- Default (asymmetric): the corner cases of cosineDistanceTransposed follow the general path';
WITH [1, -1, 1, -1]::QBit(Float32, 4) AS v
SELECT cosineDistanceTransposed(v, [0, 0, 0, 0]::Array(Float32), 1) AS zero_reference,
       cosineDistanceTransposed(v, [1, 1, 1, 1]::Array(Float32), 1) AS orthogonal,
       cosineDistanceTransposed(v, [1, -1, 1, -1]::Array(Float32), 1) AS same_direction,
       cosineDistanceTransposed(v, [-3, 3, -3, 3]::Array(Float32), 1) AS opposite_direction;

SELECT '-- Symmetric: float QBits against the explicit sign vectors (c = 2.0)';
SET qbit_one_bit_symmetric_distance = 1;
WITH
    [3.5, -1, 0.001, -0.75, 2, -2, 0, 100, -0.5, 7, -7, 0.003]::Array(Float32) AS ref,
    arrayMap(x -> if(x < 0, -2., 2.), ref) AS ref_signs,
    arrayMap(x -> if(x < 0, -2., 2.), CAST(f32, 'Array(Float32)')) AS vec_signs,
    arraySum(arrayMap((x, y) -> x != y, ref_signs, vec_signs)) AS h
SELECT id, h,
       round(L2DistanceTransposed(bf, ref, 1), 6) AS bf_l2,
       round(L2DistanceTransposed(f32, ref, 1), 6) AS f32_l2,
       round(L2DistanceTransposed(f64, ref, 1), 6) AS f64_l2,
       round(L2Distance(vec_signs, ref_signs), 6) AS expected_l2,
       round(cosineDistanceTransposed(f32, ref, 1), 6) AS f32_cos,
       round(cosineDistanceTransposed(f64, ref, 1), 6) AS f64_cos,
       round(cosineDistance(vec_signs, ref_signs), 6) AS expected_cos,
       dotProductTransposed(bf, ref, 1) AS bf_dot,
       dotProductTransposed(f32, ref, 1) AS f32_dot,
       dotProductTransposed(f64, ref, 1) AS f64_dot,
       dotProduct(vec_signs, ref_signs) AS expected_dot
FROM qbit_one_bit ORDER BY id;

SELECT '-- Symmetric: Int8 QBit against the explicit sign vectors (c = 64)';
WITH
    [3, -1, 1, -75, 2, -2, 0, 100, -50, 7, -7, 3]::Array(Int8) AS ref,
    arrayMap(x -> if(x < 0, -64., 64.), ref) AS ref_signs,
    arrayMap(x -> if(x < 0, -64., 64.), CAST(i8, 'Array(Int8)')) AS vec_signs
SELECT id,
       round(L2DistanceTransposed(i8, ref, 1), 6) AS i8_l2,
       round(L2Distance(vec_signs, ref_signs), 6) AS expected_l2,
       round(cosineDistanceTransposed(i8, ref, 1), 6) AS i8_cos,
       round(cosineDistance(vec_signs, ref_signs), 6) AS expected_cos,
       dotProductTransposed(i8, ref, 1) AS i8_dot,
       dotProduct(vec_signs, ref_signs) AS expected_dot
FROM qbit_one_bit ORDER BY id;

SELECT '-- Symmetric: the same through the partial-reads optimisation (only the sign plane is read from the table)';
SET optimize_qbit_distance_function_reads = 1;
WITH [3.5, -1, 0.001, -0.75, 2, -2, 0, 100, -0.5, 7, -7, 0.003]::Array(Float32) AS ref
SELECT id,
       round(L2DistanceTransposed(f32, ref, 1), 6) AS l2,
       round(cosineDistanceTransposed(bf, ref, 1), 6) AS cos,
       dotProductTransposed(f64, ref, 1) AS dot
FROM qbit_one_bit ORDER BY id;
SET optimize_qbit_distance_function_reads = 0;

SELECT '-- Symmetric: non-constant reference, one per row';
WITH
    arrayMap(i -> toFloat32(if(bitTest(id + 1, i), -1, 1) * (i + 1)), range(12)) AS ref,
    arrayMap(x -> if(x < 0, -2., 2.), ref) AS ref_signs,
    arrayMap(x -> if(x < 0, -2., 2.), CAST(f32, 'Array(Float32)')) AS vec_signs
SELECT id,
       round(L2DistanceTransposed(f32, ref, 1), 6) AS l2,
       round(L2Distance(vec_signs, ref_signs), 6) AS expected_l2,
       round(cosineDistanceTransposed(f32, ref, 1), 6) AS cos,
       round(cosineDistance(vec_signs, ref_signs), 6) AS expected_cos
FROM qbit_one_bit ORDER BY id;

DROP TABLE qbit_one_bit;

SELECT '-- Strided QBit, both modes: whole vector and a reduced number of dimensions';
WITH
    [1, -1, 1, -1, 1, -1, 1, -1, 1, 1, 1, 1, -1, -1, -1, -1]::QBit(Float32, 16, 8) AS v,
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]::Array(Float32) AS ref
SELECT round(L2DistanceTransposed(v, ref, 1), 6) AS l2_16, round(cosineDistanceTransposed(v, ref, 1), 6) AS cos_16, dotProductTransposed(v, ref, 1) AS dot_16,
       round(L2DistanceTransposed(v, ref, 1, 8), 6) AS l2_8, round(cosineDistanceTransposed(v, ref, 1, 8), 6) AS cos_8, dotProductTransposed(v, ref, 1, 8) AS dot_8
SETTINGS qbit_one_bit_symmetric_distance = 1;
WITH
    [1, -1, 1, -1, 1, -1, 1, -1, 1, 1, 1, 1, -1, -1, -1, -1]::QBit(Float32, 16, 8) AS v,
    [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]::Array(Float32) AS ref
SELECT round(L2DistanceTransposed(v, ref, 1), 6) AS l2_16, round(cosineDistanceTransposed(v, ref, 1), 6) AS cos_16, dotProductTransposed(v, ref, 1) AS dot_16,
       round(L2DistanceTransposed(v, ref, 1, 8), 6) AS l2_8, round(cosineDistanceTransposed(v, ref, 1, 8), 6) AS cos_8, dotProductTransposed(v, ref, 1, 8) AS dot_8
SETTINGS qbit_one_bit_symmetric_distance = 0;

SELECT '-- Symmetric: signs of zero, -0.0 is negative and +0.0 is positive, on both sides';
WITH [-0.0, 0.0, -0.0, 0.0]::QBit(Float32, 4) AS v
SELECT dotProductTransposed(v, [-0.0, 0.0, -0.0, 0.0]::Array(Float32), 1) AS all_agree,
       dotProductTransposed(v, [0.0, 0.0, 0.0, 0.0]::Array(Float32), 1) AS two_differ,
       dotProductTransposed(v, [0.0, -0.0, 0.0, -0.0]::Array(Float32), 1) AS all_differ;
WITH [-0.0, 0.0]::QBit(BFloat16, 2) AS v
SELECT dotProductTransposed(v, [-1.0, 1.0]::Array(BFloat16), 1) AS bf16_agree, dotProductTransposed(v, [1.0, -1.0]::Array(BFloat16), 1) AS bf16_differ;
WITH [-0.0, 0.0]::QBit(Float64, 2) AS v
SELECT dotProductTransposed(v, [-1.0, 1.0]::Array(Float64), 1) AS f64_agree, dotProductTransposed(v, [1.0, -1.0]::Array(Float64), 1) AS f64_differ;

SELECT '-- Symmetric: magnitudes of the reference do not matter, only its signs';
WITH [1, -1, 1, -1]::QBit(Float32, 4) AS v
SELECT L2DistanceTransposed(v, [1, -1, 1, -1]::Array(Float32), 1) AS same_signs_unit,
       L2DistanceTransposed(v, [1000, -0.001, 5, -1e10]::Array(Float32), 1) AS same_signs_any,
       L2DistanceTransposed(v, [-1, 1, -1, 1]::Array(Float32), 1) AS opposite_signs;

SELECT '-- Symmetric quantized functions: c is the Lloyd-Max 1-bit prefix centroid sqrt(2 / pi), so c^2 = 2 / pi';
WITH
    arrayMap(x -> quantizeBFloat16ToInt8(x), [0.3, -0.2, 0.1, -1, 0.7, -0.05, 2, -0.5]::Array(BFloat16))::QBit(Int8, 8) AS v,
    [1, 1, 1, 1, 1, -1, -1, -1]::Array(Float32) AS ref -- signs differ in 3 of the 8 dimensions
SELECT round(cosineDistanceTransposedQuantized(v, ref, 1), 6) AS cos,
       round(dotProductTransposedQuantized(v, ref, 1), 6) AS dot,
       round(L2DistanceTransposedQuantized(v, ref, 1), 6) AS l2,
       round(dotProductTransposedQuantized(v, ref, 1) / (8 - 2 * 3), 6) AS c_squared,
       round(2 / pi(), 6) AS two_over_pi,
       round(L2DistanceTransposedQuantized(v, ref, 1) / (2 * sqrt(3)), 6) AS c_from_l2,
       round(sqrt(2 / pi()), 6) AS sqrt_two_over_pi;

SELECT '-- Symmetric quantized functions: an Array(Int8) reference of codes contributes its signs exactly like a Float32 reference';
WITH
    arrayMap(x -> quantizeBFloat16ToInt8(x), [0.3, -0.2, 0.1, -1, 0.7, -0.05, 2, -0.5]::Array(BFloat16))::QBit(Int8, 8) AS v,
    [1, 1, 1, 1, -1, -1, -1, -1]::Array(Float32) AS ref_f32,
    arrayMap(x -> quantizeBFloat16ToInt8(x), [0.9, 0.001, 0.2, 3, -0.4, -1, -0.01, -2]::Array(BFloat16)) AS ref_i8
SELECT round(cosineDistanceTransposedQuantized(v, ref_f32, 1), 6) = round(cosineDistanceTransposedQuantized(v, ref_i8, 1), 6),
       dotProductTransposedQuantized(v, ref_f32, 1) = dotProductTransposedQuantized(v, ref_i8, 1),
       L2DistanceTransposedQuantized(v, ref_f32, 1) = L2DistanceTransposedQuantized(v, ref_i8, 1);
SET qbit_one_bit_symmetric_distance = 0;

SELECT '-- Default (asymmetric) quantized functions: codes reconstructed to +-sqrt(2 / pi) against the full-precision reference';
WITH
    arrayMap(x -> quantizeBFloat16ToInt8(x), [0.3, -0.2, 0.1, -1, 0.7, -0.05, 2, -0.5]::Array(BFloat16))::QBit(Int8, 8) AS v,
    [0.5, 0.25, -1, 2, 0.1, -0.1, 3, -0.7]::Array(Float32) AS ref,
    arrayMap(x -> if(x < 0, -1, 1) * sqrt(2 / pi()), [0.3, -0.2, 0.1, -1, 0.7, -0.05, 2, -0.5]) AS recon
SELECT round(L2DistanceTransposedQuantized(v, ref, 1), 4) AS l2, round(L2Distance(recon, ref), 4) AS expected_l2,
       round(cosineDistanceTransposedQuantized(v, ref, 1), 4) AS cos, round(cosineDistance(recon, ref), 4) AS expected_cos,
       round(dotProductTransposedQuantized(v, ref, 1), 4) AS dot, round(dotProduct(recon, ref), 4) AS expected_dot;
-- An Array(Int8) reference is dequantized at full precision before the comparison.
WITH
    arrayMap(x -> quantizeBFloat16ToInt8(x), [0.3, -0.2, 0.1, -1, 0.7, -0.05, 2, -0.5]::Array(BFloat16))::QBit(Int8, 8) AS v,
    arrayMap(x -> quantizeBFloat16ToInt8(x), [0.9, 0.001, 0.2, 3, -0.4, -1, -0.01, -2]::Array(BFloat16)) AS ref_i8,
    arrayMap(x -> toFloat32(dequantizeInt8ToBFloat16(x)), ref_i8) AS ref_dequantized,
    arrayMap(x -> if(x < 0, -1, 1) * sqrt(2 / pi()), [0.3, -0.2, 0.1, -1, 0.7, -0.05, 2, -0.5]) AS recon
SELECT round(dotProductTransposedQuantized(v, ref_i8, 1), 4) AS dot, round(dotProduct(recon, ref_dequantized), 4) AS expected_dot;
