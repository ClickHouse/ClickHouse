-- The `Quantize(method, dimensions[, bits])` codec validates its parameters at DDL time.
-- The bit-packed methods (rabitq, turboquant, e8) require `dimensions` to be a multiple of 8, and `e8` requires
-- `bits` in [1, 16] (it builds a 2^bits codebook). `int8` accepts any positive dimension.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS quantize_bad;

-- Dimensions not a multiple of 8 are rejected for the bit-packed methods.
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantize('rabitq', 65))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantize('turboquant', 60))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantize('e8', 65, 8))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- e8 requires bits in [1, 16] (0 or an unbounded value would otherwise build a degenerate or huge codebook).
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantize('e8', 64))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantize('e8', 64, 0))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantize('e8', 64, 30))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- An unknown method is rejected.
CREATE TABLE quantize_bad (id UInt32, vec Array(Float32) CODEC(Quantize('nope', 64))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_CODEC_PARAMETER }

-- int8 is not bit-packed, so a non-multiple-of-8 dimension is accepted; its code is dimensions + 4 = 69 bytes.
CREATE TABLE quantize_int8_ok (id UInt32, vec Array(Float32) CODEC(Quantize('int8', 65))) ENGINE = MergeTree ORDER BY id;
INSERT INTO quantize_int8_ok SELECT 1, arrayMap(j -> toFloat32(j), range(65));
SELECT 'int8_non_multiple_of_8_ok', length(vec.quantized) FROM quantize_int8_ok;
DROP TABLE quantize_int8_ok;
