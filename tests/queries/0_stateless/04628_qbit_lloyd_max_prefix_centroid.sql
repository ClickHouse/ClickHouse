-- A truncated Gaussian Lloyd-Max prefix is reconstructed with the conditional
-- mean of its existing fine-cell interval, rather than a middle fine-cell
-- reconstruction level. The sign pairs below also lock the raw-code prefix
-- mapping (the negative raw byte is the bitwise complement of the positive).

WITH
    [quantizeBFloat16ToInt8(0.5::BFloat16), quantizeBFloat16ToInt8(0.5::BFloat16)]::QBit(Int8, 2) AS positive,
    [quantizeBFloat16ToInt8(-0.5::BFloat16), quantizeBFloat16ToInt8(-0.5::BFloat16)]::QBit(Int8, 2) AS negative,
    [0.0, 0.0]::Array(Float32) AS zero,
    [0.5, 0.5]::Array(Float32) AS average,
    [-0.5, -0.5]::Array(Float32) AS negative_average,
    sqrt(2 / pi()) AS expected1,
    0.32705012 AS expected2,
    0.49476221 AS expected3,
    0.58414042 AS expected4,
    0.53715169 AS expected5,
    0.51431787 AS expected6,
    0.50304580 AS expected7
SELECT
    -- At precision 1 both sides are reduced to their signs and the distance is derived from the Hamming distance between the
    -- sign vectors with elements +-centroid (see 05227_qbit_one_bit_hamming_distance): two opposite 2-element sign vectors are
    -- 2 * centroid * sqrt(2) apart, and two equal ones have the dot product 2 * centroid^2.
    abs(L2DistanceTransposedQuantized(positive, negative_average, 1) / (2 * sqrt(2)) - expected1) < 1e-6 AS positive_l2_centroid,
    abs(L2DistanceTransposedQuantized(negative, average, 1) / (2 * sqrt(2)) - expected1) < 1e-6 AS negative_l2_centroid,
    -- SimSIMD's L2 kernel can differ slightly by architecture; dot product below locks the centroid itself more tightly.
    abs(L2DistanceTransposedQuantized(positive, zero, 2) / sqrt(2) - expected2) < 1e-3 AS positive_p2_l2,
    abs(L2DistanceTransposedQuantized(negative, zero, 2) / sqrt(2) - expected2) < 1e-3 AS negative_p2_l2,
    abs(L2DistanceTransposedQuantized(positive, zero, 4) / sqrt(2) - expected4) < 1e-3 AS positive_p4_l2,
    abs(L2DistanceTransposedQuantized(negative, zero, 4) / sqrt(2) - expected4) < 1e-3 AS negative_p4_l2,
    abs(dotProductTransposedQuantized(positive, average, 1) - 2 * expected1 * expected1) < 1e-6 AS positive_dot_centroid,
    abs(dotProductTransposedQuantized(negative, average, 1) + 2 * expected1 * expected1) < 1e-6 AS negative_dot_centroid,
    abs(dotProductTransposedQuantized(positive, average, 2) - expected2) < 1e-6 AS positive_p2_sign,
    abs(dotProductTransposedQuantized(negative, average, 2) + expected2) < 1e-6 AS negative_p2_sign,
    abs(dotProductTransposedQuantized(positive, average, 3) - expected3) < 1e-6 AS positive_p3_sign,
    abs(dotProductTransposedQuantized(negative, average, 3) + expected3) < 1e-6 AS negative_p3_sign,
    abs(dotProductTransposedQuantized(positive, average, 4) - expected4) < 1e-6 AS positive_p4_sign,
    abs(dotProductTransposedQuantized(negative, average, 4) + expected4) < 1e-6 AS negative_p4_sign,
    abs(dotProductTransposedQuantized(positive, average, 5) - expected5) < 1e-6 AS positive_p5_sign,
    abs(dotProductTransposedQuantized(negative, average, 5) + expected5) < 1e-6 AS negative_p5_sign,
    abs(dotProductTransposedQuantized(positive, average, 6) - expected6) < 1e-6 AS positive_p6_sign,
    abs(dotProductTransposedQuantized(negative, average, 6) + expected6) < 1e-6 AS negative_p6_sign,
    abs(dotProductTransposedQuantized(positive, average, 7) - expected7) < 1e-6 AS positive_p7_sign,
    abs(dotProductTransposedQuantized(negative, average, 7) + expected7) < 1e-6 AS negative_p7_sign;

-- Full precision must remain identical to the existing Lloyd-Max
-- reconstruction level.
WITH
    quantizeBFloat16ToInt8(0.5::BFloat16) AS code,
    [code, code]::QBit(Int8, 2) AS vector,
    [0.5, 0.5]::Array(Float32) AS average
SELECT abs(
    dotProductTransposedQuantized(vector, average, 8)
    - toFloat32(dequantizeInt8ToBFloat16(code))) < 1e-7 AS full_precision_unchanged;
