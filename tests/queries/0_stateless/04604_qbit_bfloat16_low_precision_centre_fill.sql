-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/110898
-- A QBit value truncated to `precision` bit planes is reconstructed to the centre of its coarse cell (the most
-- significant dropped bit is set), not to the cell's lower edge (dropped bits zero-filled). Zero-filling made
-- 1-bit BFloat16 reconstruction collapse to +-0.0: the reconstructed vector was the zero vector, so the distance
-- was the same for every row and carried no ranking information. With the centre fill, 1-bit reconstruction is a
-- proper sign quantization, consistent with the QBit(Int8) Lloyd-Max path (LloydMax::transposedDequantLUT).

DROP TABLE IF EXISTS qbit_recon;
CREATE TABLE qbit_recon (id UInt32, bf QBit(BFloat16, 8)) ENGINE = MergeTree ORDER BY id;

-- 5 vectors with distinct sign patterns: element i of row n is +-0.5 depending on bit i of n
INSERT INTO qbit_recon
SELECT number, arrayMap(i -> toBFloat16(0.5 * if(bitTest(number, i), 1, -1)), range(8))
FROM numbers(5);

SELECT '-- 1-bit cosine distance ranks by sign (used to be constant 1 for every row)';
SET optimize_qbit_distance_function_reads = 0;
SELECT id,
       round(cosineDistance(CAST(bf, 'Array(Float32)'), [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]), 4) AS true_cos,
       round(cosineDistanceTransposed(bf, [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5], 1), 4) AS bf16_1bit
FROM qbit_recon ORDER BY id;

SET optimize_qbit_distance_function_reads = 1;
SELECT id,
       round(cosineDistance(CAST(bf, 'Array(Float32)'), [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5]), 4) AS true_cos,
       round(cosineDistanceTransposed(bf, [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5], 1), 4) AS bf16_1bit
FROM qbit_recon ORDER BY id;

SELECT '-- 1-bit L2 distance distinguishes sign patterns too';
SELECT id,
       round(L2DistanceTransposed(bf, [0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5], 1), 4) AS bf16_1bit_l2
FROM qbit_recon ORDER BY id;

DROP TABLE qbit_recon;

-- Read out the reconstructed first coordinate through a dot product with a one-hot reference vector.
-- BFloat16 1.0 is 0x3F80 (exponent bits E = 8). At precision 1 only the sign survives and the centre fill sets the
-- top exponent bit, giving a bounded +-2.0 (not the degenerate +-0.0). At precision 8 an exponent bit is still being
-- truncated, so setting the most significant dropped bit would jump across binades; the bounded lower edge of the
-- coarse cell is kept instead, reconstructing 0.5. At precision 9 the whole exponent is kept and only mantissa bits
-- are dropped, so the most significant dropped mantissa bit is set (bounded midpoint within the binade), giving 1.5.
-- Precision 16 keeps all bits.
SELECT '-- BFloat16 reconstruction of +-1.0 at precisions 1, 8, 9, 16';
WITH [1.0, -1.0]::QBit(BFloat16, 2) AS v, [1.0, 0.0]::Array(BFloat16) AS first, [0.0, 1.0]::Array(BFloat16) AS second
SELECT dotProductTransposed(v, first, 1), dotProductTransposed(v, second, 1),
       dotProductTransposed(v, first, 8), dotProductTransposed(v, second, 8),
       dotProductTransposed(v, first, 9), dotProductTransposed(v, second, 9),
       dotProductTransposed(v, first, 16), dotProductTransposed(v, second, 16);

SELECT '-- Float32 and Float64 reconstruction of +-1.0 at precision 1';
WITH [1.0, -1.0]::QBit(Float32, 2) AS v, [1.0, 0.0]::Array(Float32) AS first, [0.0, 1.0]::Array(Float32) AS second
SELECT dotProductTransposed(v, first, 1), dotProductTransposed(v, second, 1);
WITH [1.0, -1.0]::QBit(Float64, 2) AS v, [1.0, 0.0]::Array(Float64) AS first, [0.0, 1.0]::Array(Float64) AS second
SELECT dotProductTransposed(v, first, 1), dotProductTransposed(v, second, 1);

SELECT '-- Int8 reconstruction of 100 and -100 at precision 1 (cell centre +-64, not 0/-128)';
WITH [100, -100]::QBit(Int8, 2) AS v, [1, 0]::Array(Int8) AS first, [0, 1]::Array(Int8) AS second
SELECT dotProductTransposed(v, first, 1), dotProductTransposed(v, second, 1);

-- A stored 0 must stay 0 at reduced precision, otherwise a naive centre fill of every cell turns an all-zero float
-- cell into a positive constant, injecting a fake direction into zero or padded dimensions (so reduced-precision
-- cosine distance would report identical zero vectors as maximally dissimilar). For a float at precision >= 2 the
-- all-zero cell reconstructs to exact 0: while exponent bits are truncated the bounded lower edge is kept (no centre),
-- and once only mantissa bits are dropped the all-zero cell is collapsed back to 0. The reviewer's repro then yields
-- 0, not 1.
SELECT '-- Zero cell: reduced-precision cosine of identical zero BFloat16 vectors is 0, not 1';
SELECT cosineDistanceTransposed([0.0]::QBit(BFloat16, 1), [0.0]::Array(BFloat16), 8) AS bf16_zero_cos_p8;

-- The first coordinate is exactly 0: at precision >= 2 every float type reconstructs it to 0; at precision 1 (pure sign
-- quantization) it shares the positive sign and reconstructs to the positive centre, as sign quantization requires.
SELECT '-- Zero coordinate reconstructs to 0 at precision >= 2 (to +centre at precision 1) for BFloat16, Float32, Float64';
WITH [0.0, 1.0]::QBit(BFloat16, 2) AS v, [1.0, 0.0]::Array(BFloat16) AS first
SELECT dotProductTransposed(v, first, 1) AS bf16_p1, dotProductTransposed(v, first, 2) AS bf16_p2, dotProductTransposed(v, first, 8) AS bf16_p8;
WITH [0.0, 1.0]::QBit(Float32, 2) AS v, [1.0, 0.0]::Array(Float32) AS first
SELECT dotProductTransposed(v, first, 1) AS f32_p1, dotProductTransposed(v, first, 2) AS f32_p2, dotProductTransposed(v, first, 16) AS f32_p16;
WITH [0.0, 1.0]::QBit(Float64, 2) AS v, [1.0, 0.0]::Array(Float64) AS first
SELECT dotProductTransposed(v, first, 1) AS f64_p1, dotProductTransposed(v, first, 2) AS f64_p2, dotProductTransposed(v, first, 32) AS f64_p32;

-- Int8 has no exponent, so an all-zero kept prefix is a uniform range of small non-negative codes, not a near-zero
-- magnitude: the raw Int8 path keeps the unconditional centre for its all-zero cell (0 -> +centre), unchanged.
SELECT '-- Int8 keeps the unconditional centre for its all-zero cell';
WITH [0, 100]::QBit(Int8, 2) AS v, [1, 0]::Array(Int8) AS first
SELECT dotProductTransposed(v, first, 2) AS int8_zero_p2;

-- Truncating exponent bits must keep magnitudes bounded: a smaller precision trades accuracy for speed, it must not
-- blow the value up by orders of magnitude (which would also make the squared result architecture-sensitive across
-- SIMD kernels). The most significant dropped bit is an exponent bit at these precisions, so the bounded lower edge of
-- the coarse cell is kept instead of a bit-space centre that would jump across binades. Before this fix the Float64
-- case reconstructed ~3.3e77 and the Float32 case ~1.3e5; both must now stay small and portable.
SELECT '-- Exponent-truncation reconstruction stays bounded (no magnitude explosion)';
SELECT round(L2DistanceTransposed([1, 2, 3]::QBit(Float64, 3), [1, 2, 3]::Array(Float64), 3), 1) AS f64_p3,
       round(L2DistanceTransposed([2, 2, 2]::QBit(Float32, 3), [0, 0, 0]::Array(Float32), 4), 1) AS f32_p4;

-- In the mantissa-truncation regime (precision > exponent bits) the all-zero cell is collapsed back to zero, but the
-- test for the zero cell must be made after masking off the sign bit: `-0.0` keeps only the sign bit, so a naive
-- `word != 0` check would OR the centre fill into it and reconstruct a non-zero negative subnormal instead of a signed
-- zero. Read the reconstructed first coordinate back through a dot product with a unit reference: a stored `-0.0` must
-- reconstruct to exactly 0. For Float64 the subnormal is directly visible (before the fix this returned the subnormal
-- -2^-1023 ~ -1.11e-308); for BFloat16/Float32 the SIMD kernels flush subnormals to zero, but the reconstruction was
-- equally wrong. The reviewer's cosine repro of two identical signed-zero vectors is then 0 (identical, not dissimilar).
SELECT '-- Signed zero (-0.0) reconstructs to a zero, not a subnormal (mantissa-truncation regime)';
SELECT dotProductTransposed([-0.0]::QBit(Float64, 1), [1.0]::Array(Float64), 12) AS f64_neg_zero_dot,
       dotProductTransposed([-0.0]::QBit(Float32, 1), [1.0]::Array(Float32), 9) AS f32_neg_zero_dot,
       dotProductTransposed([-0.0]::QBit(BFloat16, 1), [1.0]::Array(BFloat16), 9) AS bf16_neg_zero_dot,
       cosineDistanceTransposed([-0.0]::QBit(BFloat16, 1), [-0.0]::Array(BFloat16), 9) AS bf16_neg_zero_cos;

-- The mantissa-truncation regime must also carve out the non-finite cell: `+-inf` has an all-ones exponent and a zero
-- kept mantissa, so a naive centre fill would OR the most significant dropped mantissa bit into it and turn `+-inf` into
-- a `NaN` (0x7F80 -> 0x7FC0 for BFloat16 at precision 9), changing the IEEE category of a legitimate input. A stored
-- `+-inf` must reconstruct to `+-inf`, so its dot product with a unit reference stays `+-inf` (before the fix this
-- returned `nan`). This is the reviewer's exact BFloat16 `p = 9` repro, plus Float32/Float64.
SELECT '-- Infinity stays infinite (not NaN) in the mantissa-truncation regime';
SELECT dotProductTransposed([inf]::QBit(Float64, 1), [1.0]::Array(Float64), 12) AS f64_pos_inf_dot,
       dotProductTransposed([-inf]::QBit(Float64, 1), [1.0]::Array(Float64), 12) AS f64_neg_inf_dot,
       dotProductTransposed([inf]::QBit(Float32, 1), [1.0]::Array(Float32), 9) AS f32_pos_inf_dot,
       dotProductTransposed([inf]::QBit(BFloat16, 1), [1.0]::Array(BFloat16), 9) AS bf16_pos_inf_dot,
       dotProductTransposed([-inf]::QBit(BFloat16, 1), [1.0]::Array(BFloat16), 9) AS bf16_neg_inf_dot;

-- The centre fill must never reach the padded tail of a non-strided QBit whose dimension is not a multiple of 8. Such a
-- QBit untransposes into a buffer padded up to the next multiple of 8, and the distance kernel is asked for exactly
-- `dimension` elements. The reconstruction now centres only the real lanes, so the padded tail stays at the zero it was
-- initialised to and can never contribute to a distance. These odd-dimension (3 and 5) cases exercise the precision == 1
-- and raw-Int8 paths (which centre every real lane unconditionally) on the padded shape and pin the exact results:
--   - BFloat16 precision 1 is pure sign quantization: +value -> +2.0, -value -> -2.0.
--   - Int8 precision 1 keeps the sign and centres to +-64.
-- cosineDistanceTransposed of a vector against its own reconstruction is 0 only if the padded lanes add nothing to either
-- the dot product or the norm, so it is the sharpest guard against padding leaking into the accumulation.
SELECT '-- Odd (non-multiple-of-8) dimensions: padded lanes never contribute';
WITH [1.0, -1.0, 0.5]::QBit(BFloat16, 3) AS v
SELECT dotProductTransposed(v, [1.0, 1.0, 1.0]::Array(BFloat16), 1) AS bf16_dim3_dot,
       L2DistanceTransposed(v, [2.0, -2.0, 2.0]::Array(BFloat16), 1) AS bf16_dim3_l2,
       round(cosineDistanceTransposed(v, [2.0, -2.0, 2.0]::Array(BFloat16), 1), 4) AS bf16_dim3_cos;
WITH [1.0, -1.0, 1.0, -1.0, 1.0]::QBit(BFloat16, 5) AS v
SELECT dotProductTransposed(v, [1.0, 1.0, 1.0, 1.0, 1.0]::Array(BFloat16), 1) AS bf16_dim5_dot,
       round(cosineDistanceTransposed(v, [2.0, -2.0, 2.0, -2.0, 2.0]::Array(BFloat16), 1), 4) AS bf16_dim5_cos;
WITH [100, -100, 50]::QBit(Int8, 3) AS v
SELECT dotProductTransposed(v, [1, 0, 0]::Array(Int8), 1) AS int8_dim3_first,
       dotProductTransposed(v, [0, 1, 0]::Array(Int8), 1) AS int8_dim3_second,
       dotProductTransposed(v, [1, 1, 1]::Array(Int8), 1) AS int8_dim3_dot;

-- Explicit, documented policy for the ambiguous non-finite cell in the mantissa-truncation regime. A truncated word with
-- all exponent bits set and a zero *kept* mantissa is indistinguishable from `+-inf`: it could be a genuine `+-inf` or a
-- `NaN` whose set mantissa bits were all dropped by the precision truncation. The reconstruction preserves the canonical
-- infinity exactly rather than fabricate a `NaN` (which would corrupt a real `+-inf`), so:
--   - A `NaN` keeps its `NaN` category only when a set mantissa bit survives the truncation. A canonical quiet `NaN` has
--     its top (quiet) mantissa bit set, which is the first mantissa bit and is kept as soon as one mantissa bit is kept:
--     precision E+2 (BFloat16/Float32 -> 10, Float64 -> 13) keeps it, so the word stays a `NaN` and the dot product is
--     `nan`.
--   - A `NaN` whose payload lies entirely in the dropped bits reconstructs to `+-inf`. At precision E+1 (BFloat16/Float32
--     -> 9, Float64 -> 12) no mantissa bit is kept, so the canonical `NaN` truncates to the `+-inf` encoding and the dot
--     product is `inf`, not `nan`. This is the reviewer's exact repro; it is intended, documented, lossy behavior, not a
--     preserved payload.
SELECT '-- NaN policy in the mantissa-truncation regime: NaN with kept payload bit stays NaN, NaN with dropped payload -> inf';
SELECT dotProductTransposed([nan]::QBit(BFloat16, 1), [1.0]::Array(BFloat16), 10) AS bf16_nan_kept,
       dotProductTransposed([nan]::QBit(BFloat16, 1), [1.0]::Array(BFloat16), 9) AS bf16_nan_dropped,
       dotProductTransposed([nan]::QBit(Float32, 1), [1.0]::Array(Float32), 10) AS f32_nan_kept,
       dotProductTransposed([nan]::QBit(Float32, 1), [1.0]::Array(Float32), 9) AS f32_nan_dropped,
       dotProductTransposed([nan]::QBit(Float64, 1), [1.0]::Array(Float64), 13) AS f64_nan_kept,
       dotProductTransposed([nan]::QBit(Float64, 1), [1.0]::Array(Float64), 12) AS f64_nan_dropped;

-- Boundary case of the CalcT downcast: a Float32 element at precision 16 still drops 16 mantissa bits, but a downcast to
-- BFloat16 would make the calculation word exactly `precision` bits wide, so the midpoint (the most significant dropped
-- bit) would not be representable and the value would silently reconstruct to the lower edge of its cell. The dispatch
-- therefore downcasts only while `precision` is strictly below the narrow width: Float32 at precision 16 computes in full
-- Float32, where `[1.0]` reconstructs to the midpoint 1.00390625 of its `0x3F800000..0x3F80FFFF` cell, not to 1.0. A
-- BFloat16 element at precision 16 drops nothing and must stay exactly 1.0; Float64 at precision 16 never downcasts and
-- centres within its own cell.
SELECT '-- Float32 precision 16 (downcast boundary): midpoint applied, not the lower edge';
SELECT dotProductTransposed([1.0]::QBit(Float32, 1), [1.0]::Array(Float32), 16) AS f32_p16_midpoint,
       dotProductTransposed([1.0]::QBit(Float32, 1), [1.0]::Array(Float32), 15) AS f32_p15_midpoint,
       dotProductTransposed([1.0]::QBit(BFloat16, 1), [1.0]::Array(BFloat16), 16) AS bf16_p16_exact,
       dotProductTransposed([1.0]::QBit(Float64, 1), [1.0]::Array(Float64), 16) AS f64_p16_midpoint;
