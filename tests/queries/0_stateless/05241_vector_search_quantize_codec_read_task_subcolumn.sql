-- The `quantized` companion subcolumn of a `Quantized(...)` column is exposed only by the custom serialization of the
-- column. Planning the read task of a part must still find it in the part (which holds that serialization), so reading
-- `vec.quantized` reads only the codes and does not inject the full `vec` column, including under PREWHERE and after
-- the part is loaded back from disk.

SET enable_quantized_codec = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS quantize_read_task;
CREATE TABLE quantize_read_task
(
    id UInt32,
    tag UInt8,
    vec Array(Float32) CODEC(Quantized('int8', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO quantize_read_task
SELECT number, number % 3, arrayMap(j -> toFloat32(sipHash64(number, j) % 100), range(64))
FROM numbers(20000);

DETACH TABLE quantize_read_task;
ATTACH TABLE quantize_read_task;

SELECT sum(vec[1]) FROM quantize_read_task FORMAT Null SETTINGS log_comment = '05241_full';
SELECT sum(length(vec.quantized)) FROM quantize_read_task FORMAT Null SETTINGS log_comment = '05241_codes';
SELECT count() FROM quantize_read_task PREWHERE length(vec.quantized) > 0 FORMAT Null SETTINGS log_comment = '05241_codes_prewhere';

SYSTEM FLUSH LOGS query_log;

-- The codes take 68 bytes per row, the vector 256, so reading the full column instead would be at least as large.
WITH
    (SELECT ProfileEvents['ReadCompressedBytes'] FROM system.query_log
     WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05241_full') AS full
SELECT log_comment, ProfileEvents['ReadCompressedBytes'] * 2 < full
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05241_codes', '05241_codes_prewhere')
ORDER BY log_comment;

DROP TABLE quantize_read_task;
