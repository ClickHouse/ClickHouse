-- The transposed distance functions (L2DistanceTransposed, cosineDistanceTransposed, dotProductTransposed) accept a reference vector
-- longer than the number of dimensions they read: the trailing elements beyond `used_dims` (or beyond the QBit dimension in the 3-argument
-- form) are ignored. This lets a full-size query vector be reused for a reduced-dimension (Matryoshka) search without slicing it first.
--
-- Every check compares the result of passing the full-length reference vector against the result of passing an explicitly-sliced one of
-- exactly the required length. The two must be bit-identical, so `=` returns 1 for every row and `min(...)` over the table is 1. We cover
-- strided and non-strided QBit, constant and non-constant reference vectors, the 3- and 4-argument forms, all three functions, and both
-- states of the partial-reads optimization. Companion to 04491_qbit_stride_distance and 04494_qbit_stride_dot_product_transposed.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS qbit_strided;
DROP TABLE IF EXISTS qbit_plain;
DROP TABLE IF EXISTS qbit_i8;
CREATE TABLE qbit_strided (id UInt32, vec QBit(Float32, 16, 8)) ENGINE = Memory;
CREATE TABLE qbit_plain (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO qbit_strided VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]), (2, [16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1]);
-- qbit_plain holds only the first 8 dimensions of each vector.
INSERT INTO qbit_plain VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [16, 15, 14, 13, 12, 11, 10, 9]);

-- Non-strided QBit(Float32, 8), 3-argument form: a 20-element reference is truncated to the QBit dimension (8).
SELECT 'nonstrided 3-arg, const ref (L2, cosine, dot)';
WITH arrayMap(i -> toFloat32(i), range(20)) AS full
SELECT
    min(L2DistanceTransposed(vec, full, 32) = L2DistanceTransposed(vec, arraySlice(full, 1, 8), 32)),
    min(cosineDistanceTransposed(vec, full, 32) = cosineDistanceTransposed(vec, arraySlice(full, 1, 8), 32)),
    min(dotProductTransposed(vec, full, 32) = dotProductTransposed(vec, arraySlice(full, 1, 8), 32))
FROM qbit_plain;

SELECT 'nonstrided 3-arg, non-const ref (L2, cosine, dot)';
WITH materialize(arrayMap(i -> toFloat32(i), range(20))) AS full
SELECT
    min(L2DistanceTransposed(vec, full, 32) = L2DistanceTransposed(vec, arraySlice(full, 1, 8), 32)),
    min(cosineDistanceTransposed(vec, full, 32) = cosineDistanceTransposed(vec, arraySlice(full, 1, 8), 32)),
    min(dotProductTransposed(vec, full, 32) = dotProductTransposed(vec, arraySlice(full, 1, 8), 32))
FROM qbit_plain;

-- Strided QBit(Float32, 16, 8), 4-argument form: a 20-element reference is truncated to used_dims (8).
SELECT 'strided 4-arg used_dims=8, const ref (L2, cosine, dot)';
WITH arrayMap(i -> toFloat32(i), range(20)) AS full
SELECT
    min(L2DistanceTransposed(vec, full, 32, 8) = L2DistanceTransposed(vec, arraySlice(full, 1, 8), 32, 8)),
    min(cosineDistanceTransposed(vec, full, 32, 8) = cosineDistanceTransposed(vec, arraySlice(full, 1, 8), 32, 8)),
    min(dotProductTransposed(vec, full, 32, 8) = dotProductTransposed(vec, arraySlice(full, 1, 8), 32, 8))
FROM qbit_strided;

SELECT 'strided 4-arg used_dims=8, non-const ref (L2, cosine, dot)';
WITH materialize(arrayMap(i -> toFloat32(i), range(20))) AS full
SELECT
    min(L2DistanceTransposed(vec, full, 32, 8) = L2DistanceTransposed(vec, arraySlice(full, 1, 8), 32, 8)),
    min(cosineDistanceTransposed(vec, full, 32, 8) = cosineDistanceTransposed(vec, arraySlice(full, 1, 8), 32, 8)),
    min(dotProductTransposed(vec, full, 32, 8) = dotProductTransposed(vec, arraySlice(full, 1, 8), 32, 8))
FROM qbit_strided;

-- Strided QBit, 3-argument form reads all 16 dimensions: a 20-element reference is truncated to 16.
SELECT 'strided 3-arg (all 16 dims), const ref (L2, cosine, dot)';
WITH arrayMap(i -> toFloat32(i), range(20)) AS full
SELECT
    min(L2DistanceTransposed(vec, full, 32) = L2DistanceTransposed(vec, arraySlice(full, 1, 16), 32)),
    min(cosineDistanceTransposed(vec, full, 32) = cosineDistanceTransposed(vec, arraySlice(full, 1, 16), 32)),
    min(dotProductTransposed(vec, full, 32) = dotProductTransposed(vec, arraySlice(full, 1, 16), 32))
FROM qbit_strided;

-- The same must hold with the partial-reads optimization disabled (the user-facing fallback path).
SET optimize_qbit_distance_function_reads = 0;
SELECT 'strided 4-arg used_dims=8, optimization off (L2, cosine, dot)';
WITH arrayMap(i -> toFloat32(i), range(20)) AS full
SELECT
    min(L2DistanceTransposed(vec, full, 32, 8) = L2DistanceTransposed(vec, arraySlice(full, 1, 8), 32, 8)),
    min(cosineDistanceTransposed(vec, full, 32, 8) = cosineDistanceTransposed(vec, arraySlice(full, 1, 8), 32, 8)),
    min(dotProductTransposed(vec, full, 32, 8) = dotProductTransposed(vec, arraySlice(full, 1, 8), 32, 8))
FROM qbit_strided;
SET optimize_qbit_distance_function_reads = 1;

-- Int8 QBit strided over 16 dims with stride 8, mirroring `dotProductTransposed(embedding_int, target_i8, 8, 16)`: a full-size Int8
-- reference vector is truncated to the requested number of dimensions.
CREATE TABLE qbit_i8 (id UInt32, vec QBit(Int8, 16, 8)) ENGINE = Memory;
INSERT INTO qbit_i8 VALUES (1, [-8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7]), (2, [7, 6, 5, 4, 3, 2, 1, 0, -1, -2, -3, -4, -5, -6, -7, -8]);

SELECT 'strided Int8 4-arg used_dims=8, const ref (dot, L2, cosine)';
WITH arrayMap(i -> toInt8(i - 10), range(20)) AS full
SELECT
    min(dotProductTransposed(vec, full, 8, 8) = dotProductTransposed(vec, arraySlice(full, 1, 8), 8, 8)),
    min(L2DistanceTransposed(vec, full, 8, 8) = L2DistanceTransposed(vec, arraySlice(full, 1, 8), 8, 8)),
    min(cosineDistanceTransposed(vec, full, 8, 8) = cosineDistanceTransposed(vec, arraySlice(full, 1, 8), 8, 8))
FROM qbit_i8;

-- Concrete values: truncating a full 16-element reference to the first 8 strided dims equals a non-strided search over those 8 dims.
SELECT 'concrete: strided (first 8 dims, ref truncated) dot product';
WITH [toFloat32(1), 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] AS full
SELECT id, round(dotProductTransposed(vec, full, 32, 8), 4) FROM qbit_strided ORDER BY id;
SELECT 'concrete: non-strided baseline over the same 8 dims';
WITH [toFloat32(1), 2, 3, 4, 5, 6, 7, 8] AS ref
SELECT id, round(dotProductTransposed(vec, ref, 32), 4) FROM qbit_plain ORDER BY id;

DROP TABLE qbit_strided;
DROP TABLE qbit_plain;
DROP TABLE qbit_i8;

-- The quantized transposed distance functions (L2DistanceTransposedQuantized, cosineDistanceTransposedQuantized,
-- dotProductTransposedQuantized) share the same reference-vector handling, so an oversized reference — the full-precision Float32
-- query or a quantized Array(Int8) query — is truncated to `used_dims` (or to the QBit dimension in the 3-argument form) as well.
DROP TABLE IF EXISTS qbit_quant;
DROP TABLE IF EXISTS qbit_quant_plain;
CREATE TABLE qbit_quant (id UInt32, vec QBit(Int8, 16, 8)) ENGINE = Memory;
INSERT INTO qbit_quant
SELECT id, codes::QBit(Int8, 16, 8)
FROM
(
    SELECT 1 AS id, arrayMap(x -> quantizeBFloat16ToInt8(toBFloat16(x)), arrayMap(i -> (i - 8) / 10, range(16))) AS codes
    UNION ALL
    SELECT 2 AS id, arrayMap(x -> quantizeBFloat16ToInt8(toBFloat16(x)), arrayMap(i -> (8 - i) / 10, range(16))) AS codes
)
ORDER BY id;

SELECT 'quantized strided 4-arg used_dims=8, const Float32 ref (L2, cosine, dot)';
WITH arrayMap(i -> (i - 4) / 10, range(20))::Array(Float32) AS full
SELECT
    min(L2DistanceTransposedQuantized(vec, full, 8, 8) = L2DistanceTransposedQuantized(vec, arraySlice(full, 1, 8), 8, 8)),
    min(cosineDistanceTransposedQuantized(vec, full, 8, 8) = cosineDistanceTransposedQuantized(vec, arraySlice(full, 1, 8), 8, 8)),
    min(dotProductTransposedQuantized(vec, full, 8, 8) = dotProductTransposedQuantized(vec, arraySlice(full, 1, 8), 8, 8))
FROM qbit_quant;

SELECT 'quantized strided 4-arg used_dims=8, non-const Float32 ref (L2, cosine, dot)';
WITH materialize(arrayMap(i -> (i - 4) / 10, range(20))::Array(Float32)) AS full
SELECT
    min(L2DistanceTransposedQuantized(vec, full, 8, 8) = L2DistanceTransposedQuantized(vec, arraySlice(full, 1, 8), 8, 8)),
    min(cosineDistanceTransposedQuantized(vec, full, 8, 8) = cosineDistanceTransposedQuantized(vec, arraySlice(full, 1, 8), 8, 8)),
    min(dotProductTransposedQuantized(vec, full, 8, 8) = dotProductTransposedQuantized(vec, arraySlice(full, 1, 8), 8, 8))
FROM qbit_quant;

SELECT 'quantized strided 4-arg used_dims=8, const Array(Int8) ref (L2, cosine, dot)';
WITH arrayMap(x -> quantizeBFloat16ToInt8(toBFloat16(x)), arrayMap(i -> (i - 10) / 10, range(20))) AS full_codes
SELECT
    min(L2DistanceTransposedQuantized(vec, full_codes, 8, 8) = L2DistanceTransposedQuantized(vec, arraySlice(full_codes, 1, 8), 8, 8)),
    min(cosineDistanceTransposedQuantized(vec, full_codes, 8, 8) = cosineDistanceTransposedQuantized(vec, arraySlice(full_codes, 1, 8), 8, 8)),
    min(dotProductTransposedQuantized(vec, full_codes, 8, 8) = dotProductTransposedQuantized(vec, arraySlice(full_codes, 1, 8), 8, 8))
FROM qbit_quant;

SET optimize_qbit_distance_function_reads = 0;
SELECT 'quantized strided 4-arg used_dims=8, optimization off (L2, cosine, dot)';
WITH arrayMap(i -> (i - 4) / 10, range(20))::Array(Float32) AS full
SELECT
    min(L2DistanceTransposedQuantized(vec, full, 8, 8) = L2DistanceTransposedQuantized(vec, arraySlice(full, 1, 8), 8, 8)),
    min(cosineDistanceTransposedQuantized(vec, full, 8, 8) = cosineDistanceTransposedQuantized(vec, arraySlice(full, 1, 8), 8, 8)),
    min(dotProductTransposedQuantized(vec, full, 8, 8) = dotProductTransposedQuantized(vec, arraySlice(full, 1, 8), 8, 8))
FROM qbit_quant;
SET optimize_qbit_distance_function_reads = 1;

CREATE TABLE qbit_quant_plain (id UInt32, vec QBit(Int8, 8)) ENGINE = Memory;
INSERT INTO qbit_quant_plain
SELECT id, codes::QBit(Int8, 8)
FROM
(
    SELECT 1 AS id, arrayMap(x -> quantizeBFloat16ToInt8(toBFloat16(x)), arrayMap(i -> (i - 4) / 10, range(8))) AS codes
    UNION ALL
    SELECT 2 AS id, arrayMap(x -> quantizeBFloat16ToInt8(toBFloat16(x)), arrayMap(i -> (4 - i) / 10, range(8))) AS codes
)
ORDER BY id;

SELECT 'quantized nonstrided 3-arg, const Float32 ref (L2, cosine, dot)';
WITH arrayMap(i -> (i - 4) / 10, range(20))::Array(Float32) AS full
SELECT
    min(L2DistanceTransposedQuantized(vec, full, 8) = L2DistanceTransposedQuantized(vec, arraySlice(full, 1, 8), 8)),
    min(cosineDistanceTransposedQuantized(vec, full, 8) = cosineDistanceTransposedQuantized(vec, arraySlice(full, 1, 8), 8)),
    min(dotProductTransposedQuantized(vec, full, 8) = dotProductTransposedQuantized(vec, arraySlice(full, 1, 8), 8))
FROM qbit_quant_plain;

DROP TABLE qbit_quant;
DROP TABLE qbit_quant_plain;
