-- Ensure that quantized sub column read does not fetch the full column.

SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS quantize_subcolumn_read_bytes;

CREATE TABLE quantize_subcolumn_read_bytes
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('product', 64, 8, 8))
)
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 8192, min_bytes_for_wide_part = 0;

-- 8 code bytes per vector against 256 bytes of full-precision data.
INSERT INTO quantize_subcolumn_read_bytes
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64))
FROM numbers(30000);

-- A fresh part reads correctly even with the bug, so the reload is what makes this a test.
DETACH TABLE quantize_subcolumn_read_bytes;
ATTACH TABLE quantize_subcolumn_read_bytes;

SELECT sum(cityHash64(vec.quantized)) FROM quantize_subcolumn_read_bytes
SETTINGS log_comment = '02354_codes' FORMAT Null;

SELECT sum(cityHash64(vec.product_quantization_codebook)) FROM quantize_subcolumn_read_bytes
SETTINGS log_comment = '02354_codebook' FORMAT Null;

SELECT sum(cityHash64(vec)) FROM quantize_subcolumn_read_bytes
SETTINGS log_comment = '02354_full' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- Compare against the full-column read, not absolute bytes, so nothing needs retuning later.
WITH
    (SELECT read_bytes FROM system.query_log
     WHERE current_database = currentDatabase() AND event_date >= yesterday()
       AND type = 'QueryFinish' AND log_comment = '02354_codes'
     ORDER BY event_time_microseconds DESC LIMIT 1) AS codes,
    (SELECT read_bytes FROM system.query_log
     WHERE current_database = currentDatabase() AND event_date >= yesterday()
       AND type = 'QueryFinish' AND log_comment = '02354_codebook'
     ORDER BY event_time_microseconds DESC LIMIT 1) AS codebook,
    (SELECT read_bytes FROM system.query_log
     WHERE current_database = currentDatabase() AND event_date >= yesterday()
       AND type = 'QueryFinish' AND log_comment = '02354_full'
     ORDER BY event_time_microseconds DESC LIMIT 1) AS full_column
SELECT
    'codes_read_is_small', codes > 0 AND codes * 4 < full_column,
    'codebook_read_is_small', codebook > 0 AND codebook * 4 < full_column;

DROP TABLE quantize_subcolumn_read_bytes SYNC;
