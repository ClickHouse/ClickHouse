-- The `quantized` companion subcolumn of a `Quantized(...)` column is exposed only by the custom serialization of the
-- column. Planning the read task of a part must still find it in the part (which holds that serialization), so reading
-- `vec.quantized` reads only the codes and does not inject the full `vec` column, including under PREWHERE and after
-- the part is loaded back from disk.

SET enable_quantized_codec = 1;
SET log_queries = 1;

DROP TABLE IF EXISTS quantize_read_task;
-- Every granule starts a new compressed block, so read tasks over different granules never read the same block.
CREATE TABLE quantize_read_task
(
    id UInt32,
    tag UInt8,
    vec Array(Float32) CODEC(Quantized('int8', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_compress_block_size = 1;

INSERT INTO quantize_read_task
SELECT number, number % 3, arrayMap(j -> toFloat32(sipHash64(number, j) % 100), range(64))
FROM numbers(20000);

DETACH TABLE quantize_read_task;
ATTACH TABLE quantize_read_task;

SELECT sum(vec[1]) FROM quantize_read_task FORMAT Null SETTINGS log_comment = '05241_full';
-- A cache hit reads no compressed bytes, so it would hide a read of the full `vec`.
SELECT sum(length(vec.quantized)) FROM quantize_read_task FORMAT Null
SETTINGS log_comment = '05241_codes', use_uncompressed_cache = 0, use_columns_cache = 0;
SELECT count() FROM quantize_read_task PREWHERE length(vec.quantized) > 0 FORMAT Null
SETTINGS log_comment = '05241_codes_prewhere', use_uncompressed_cache = 0, use_columns_cache = 0;

SYSTEM FLUSH LOGS query_log;

-- The codes take 68 bytes per row, the vector 256, so reading the full column instead would be at least as large.
-- With parallel replicas each replica logs the granules it read in its own row, so the rows of a query are summed.
WITH read_bytes AS
(
    SELECT anyIf(log_comment, is_initial_query) AS comment, sum(ProfileEvents['ReadCompressedBytes']) AS bytes
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish'
      AND initial_query_id IN
      (
          SELECT query_id FROM system.query_log
          WHERE current_database = currentDatabase() AND event_date >= yesterday()
            AND type = 'QueryFinish' AND is_initial_query
            AND log_comment IN ('05241_full', '05241_codes', '05241_codes_prewhere')
          ORDER BY event_time_microseconds DESC
          LIMIT 1 BY log_comment
      )
    GROUP BY initial_query_id
)
SELECT comment, bytes * 2 < (SELECT bytes FROM read_bytes WHERE comment = '05241_full')
FROM read_bytes
WHERE comment != '05241_full'
ORDER BY comment;

DROP TABLE quantize_read_task;

-- The same in a compact part with substream marks, where several subcolumns of `vec` are read in the order of their
-- substreams: the result must not change after the part is loaded back from disk.
DROP TABLE IF EXISTS quantize_read_task_compact;
CREATE TABLE quantize_read_task_compact
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('int8', 64))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 1000000000, write_marks_for_substreams_in_compact_parts = 1;

INSERT INTO quantize_read_task_compact
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 100), range(64))
FROM numbers(20000);

SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 'quantize_read_task_compact' AND active;
SELECT sum(length(vec.quantized)), sum(vec.size0), groupBitXor(cityHash64(id, vec.quantized)) FROM quantize_read_task_compact;

DETACH TABLE quantize_read_task_compact;
ATTACH TABLE quantize_read_task_compact;

SELECT sum(length(vec.quantized)), sum(vec.size0), groupBitXor(cityHash64(id, vec.quantized)) FROM quantize_read_task_compact;
SELECT sum(vec.size0), sum(length(vec.quantized)), groupBitXor(cityHash64(id, vec.quantized)) FROM quantize_read_task_compact;
SELECT count() FROM quantize_read_task_compact PREWHERE length(vec.quantized) > 0 WHERE vec.size0 = 64;

DROP TABLE quantize_read_task_compact;
