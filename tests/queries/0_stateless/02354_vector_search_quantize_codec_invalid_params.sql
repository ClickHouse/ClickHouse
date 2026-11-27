-- Tags: no-parallel-replicas
-- (the quantized companion subcolumn `<column>.quantized` is not read on the parallel-replicas path - it comes back
--  empty - so the `length(vec.quantized)` checks below cannot hold there; this matches the other Quantized-codec tests.)
-- The `Quantized(method, dimensions[, ...])` codec validates its parameters at DDL time.
-- The bit-packed methods (rabitq, turboquant) require `dimensions` to be a multiple of 8; `int8` accepts any positive
-- dimension. The `prefix` method takes Quantized('prefix', dimensions, leading_dimensions, 'int8'|'bf16').

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS quantize_bad;

-- Dimensions not a multiple of 8 are rejected for the bit-packed methods.
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 65))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantized('turboquant', 60))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- An unknown method is rejected.
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantized('nope', 64))) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_DATA }

-- `prefix` requires the 4-argument form with a format of 'int8' or 'bf16'.
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantized('prefix', 64, 32))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantized('prefix', 64, 32, 'fp8'))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }
-- The number of leading dimensions must be in [1, dimensions].
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantized('prefix', 64, 0, 'int8'))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantized('prefix', 64, 128, 'int8'))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- int8 is not bit-packed, so a non-multiple-of-8 dimension is accepted; its code is dimensions + 4 = 69 bytes.
CREATE TABLE quantize_int8_ok (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 65))) ENGINE = MergeTree ORDER BY id;
INSERT INTO quantize_int8_ok SELECT 1, arrayMap(j -> toFloat32(j), range(65));
SELECT 'int8_non_multiple_of_8_ok', length(vec.quantized) FROM quantize_int8_ok;
DROP TABLE quantize_int8_ok;

-- mrl int8: stores the leading 32 dims as int8 + a 4-byte scale = 36 bytes. mrl bf16: 32 * 2 = 64 bytes.
CREATE TABLE quantize_mrl_ok (id UInt32, vi Array(Float32) CODEC(Quantized('prefix', 64, 32, 'int8')), vb Array(Float32) CODEC(Quantized('prefix', 64, 32, 'bf16'))) ENGINE = MergeTree ORDER BY id;
INSERT INTO quantize_mrl_ok SELECT 1, arrayMap(j -> toFloat32(j), range(64)), arrayMap(j -> toFloat32(j), range(64));
SELECT 'prefix_int8_ok', length(vi.quantized), 'prefix_bf16_ok', length(vb.quantized) FROM quantize_mrl_ok;
DROP TABLE quantize_mrl_ok;
