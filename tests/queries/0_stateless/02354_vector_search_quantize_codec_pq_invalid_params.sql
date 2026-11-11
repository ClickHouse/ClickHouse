-- Tags: no-parallel-replicas
-- (the quantized companion subcolumn `<column>.quantized` is not read on the parallel-replicas path - it comes back
--  empty - so the `length(vec.quantized)` check below cannot hold there; this matches the other Quantized-codec tests.)
-- The `product` method of the `Quantized(...)` codec uses the syntax `Quantized('product', dimensions, nbits, m)` and validates its
-- parameters at DDL time: it requires exactly four arguments, `dimensions` a multiple of `m`, and `nbits` in [1, 16].

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS quantize_pq_bad;

-- `product` requires the 4-argument form; the 2- and 3-argument forms are rejected.
CREATE TABLE quantize_pq_bad (id UInt32, vec Array(Float32) CODEC(Quantized('product', 64))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }
CREATE TABLE quantize_pq_bad (id UInt32, vec Array(Float32) CODEC(Quantized('product', 64, 8))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

-- `dimensions` must be a multiple of `m` (64 is not a multiple of 7).
CREATE TABLE quantize_pq_bad (id UInt32, vec Array(Float32) CODEC(Quantized('product', 64, 8, 7))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- `m` must be greater than zero.
CREATE TABLE quantize_pq_bad (id UInt32, vec Array(Float32) CODEC(Quantized('product', 64, 8, 0))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- `nbits` must be in [1, 16] (it builds a 2^nbits codebook per subspace).
CREATE TABLE quantize_pq_bad (id UInt32, vec Array(Float32) CODEC(Quantized('product', 64, 0, 8))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE quantize_pq_bad (id UInt32, vec Array(Float32) CODEC(Quantized('product', 64, 17, 8))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- The 4th argument (m) is only valid for `product`; the data-independent methods reject it.
CREATE TABLE quantize_pq_bad (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 64, 8, 8))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

-- The per-part codebook is a FixedString(2^nbits * dimensions * 4); parameter combinations whose codebook exceeds the
-- FixedString limit (0xFFFFFF bytes) are rejected at DDL time. For 64 dimensions and nbits = 16 that is
-- 65536 * 64 * 4 = 16777216 bytes, just over the limit.
CREATE TABLE quantize_pq_bad (id UInt32, vec Array(Float32) CODEC(Quantized('product', 64, 16, 8))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- A valid 4-argument `product` codec is accepted and its code is m = 8 bytes (one byte per subspace at nbits = 8).
CREATE TABLE quantize_pq_ok (id UInt32, vec Array(Float32) CODEC(Quantized('product', 64, 8, 8))) ENGINE = MergeTree ORDER BY id;
INSERT INTO quantize_pq_ok SELECT 1, arrayMap(j -> toFloat32(j), range(64));
SELECT 'pq_ok_code_length', length(vec.quantized) FROM quantize_pq_ok;
DROP TABLE quantize_pq_ok;
