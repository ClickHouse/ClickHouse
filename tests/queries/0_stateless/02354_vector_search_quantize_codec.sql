-- The `Quantized(method, dimensions[, bits])` column codec stores a compact, data-independent quantized companion
-- stream of a dense vector column, exposed as the readable subcolumn `<column>.quantized`. The full-precision data is
-- stored verbatim (the codec is a no-op at the byte level), so reading the vector itself is unaffected.
-- The codec is gated behind `allow_experimental_codecs`.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS quantize_codec;
CREATE TABLE quantize_codec
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('rabitq', 64))
)
ENGINE = MergeTree ORDER BY id;

-- The codec round-trips through SHOW CREATE.
SELECT 'show_create_has_codec', position(create_table_query, 'Quantized(\'rabitq\', 64') > 0
FROM system.tables WHERE database = currentDatabase() AND name = 'quantize_codec';

INSERT INTO quantize_codec (id, vec)
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(100);

-- The quantized subcolumn is readable and has the expected fixed code size.
SELECT 'subcolumn_type', toTypeName(vec.quantized) FROM quantize_codec LIMIT 1;
SELECT 'code_length', length(vec.quantized) FROM quantize_codec GROUP BY length(vec.quantized);

-- The full-precision vectors round-trip unchanged.
SELECT 'full_precision_rows', count() FROM quantize_codec WHERE length(vec) = 64;

DROP TABLE quantize_codec;

-- Without `allow_experimental_codecs` the codec is rejected.
SET allow_experimental_codecs = 0;
CREATE TABLE quantize_codec_gated (id UInt32, vec Array(Float32) CODEC(Quantized('rabitq', 64)))
ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
