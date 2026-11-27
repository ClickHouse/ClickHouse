-- The `Quantized(method, dimensions[, ...])` column codec is only valid on an Array(Float32|Float64|BFloat16) column,
-- and every stored vector must have exactly `dimensions` elements. This test pins down the error surface: the type is
-- rejected at DDL time, the per-vector length is rejected at write time, and the codec cannot be chained with a
-- compression codec.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS quantize_neg;

-- DDL-time: the Quantized codec is only supported on Array(Float32|Float64|BFloat16). A non-Array column, an Array of a
-- non-float element type, or a nested Nullable/LowCardinality element is rejected at CREATE TABLE with ILLEGAL_COLUMN.
CREATE TABLE quantize_neg (id UInt32, vec UInt32 CODEC(Quantized('int8', 64))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE quantize_neg (id UInt32, vec Array(Int32) CODEC(Quantized('int8', 64))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE quantize_neg (id UInt32, vec Array(String) CODEC(Quantized('int8', 64))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE quantize_neg (id UInt32, vec Array(Nullable(Float32)) CODEC(Quantized('int8', 64))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }
CREATE TABLE quantize_neg (id UInt32, vec Array(LowCardinality(Float32)) CODEC(Quantized('int8', 64))) ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_COLUMN }

-- DDL-time: the Quantized codec is a NONE-category codec, so it cannot be chained with a compression codec.
CREATE TABLE quantize_neg (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 64), ZSTD)) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

-- Write-time: every stored vector must have exactly `dimensions` elements, checked while encoding the codes on INSERT.
CREATE TABLE quantize_neg (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 64))) ENGINE = MergeTree ORDER BY id;

-- A vector shorter than the declared dimensions is rejected.
INSERT INTO quantize_neg SELECT 1, arrayMap(j -> toFloat32(j), range(32)); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
-- An empty vector is rejected (0 != 64).
INSERT INTO quantize_neg SELECT 1, []::Array(Float32); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
-- A block that mixes a correct-length row with a wrong-length row is rejected as a whole (the write is atomic).
INSERT INTO quantize_neg VALUES (1, arrayMap(j -> toFloat32(j), range(64))), (2, arrayMap(j -> toFloat32(j), range(63))); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

-- A correct-length vector still inserts, and the failed inserts above left no rows behind.
INSERT INTO quantize_neg SELECT 1, arrayMap(j -> toFloat32(j), range(64));
SELECT 'rows_after_failed_inserts', count() FROM quantize_neg;

DROP TABLE quantize_neg;
