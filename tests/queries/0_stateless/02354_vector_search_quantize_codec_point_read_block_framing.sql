-- Tags: no-parallel-replicas
-- The point read addresses a row arithmetically, so the writer has to really emit one vector per compressed block.
-- An adaptive write buffer starts below the maximum and would quietly break that.

SET enable_quantized_codec = 1;
SET vector_search_use_quantized_codes = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000000;

-- An adaptive write buffer starts below `max_compress_block_size`, which would make the first block hold less
-- than one vector and shift every row boundary after it. The two tables below differ only in that buffer, so
-- their `vec` must come out byte-identical; before the fix the adaptive one carried an extra block of framing.
DROP TABLE IF EXISTS quantize_pr_framing_adaptive;
DROP TABLE IF EXISTS quantize_pr_framing_plain;

CREATE TABLE quantize_pr_framing_adaptive (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 64)))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         quantized_vector_one_block_per_row = 1,
         min_columns_to_activate_adaptive_write_buffer = 1, adaptive_write_buffer_initial_size = 128;

CREATE TABLE quantize_pr_framing_plain (id UInt32, vec Array(Float32) CODEC(Quantized('int8', 64)))
ENGINE = MergeTree ORDER BY id
SETTINGS index_granularity = 512, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         quantized_vector_one_block_per_row = 1,
         min_columns_to_activate_adaptive_write_buffer = 0;

INSERT INTO quantize_pr_framing_adaptive
SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)) FROM numbers(100);
INSERT INTO quantize_pr_framing_plain SELECT * FROM quantize_pr_framing_adaptive;

-- `column_bytes_on_disk` covers every substream of `vec`, and the adaptive buffer still applies to the other
-- two, so the elements stream is isolated by subtracting the subcolumns.
SELECT 'adaptive_buffer_keeps_layout',
    (SELECT column_bytes_on_disk - arraySum(subcolumns.bytes_on_disk) FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 'quantize_pr_framing_adaptive' AND active AND column = 'vec')
  = (SELECT column_bytes_on_disk - arraySum(subcolumns.bytes_on_disk) FROM system.parts_columns
     WHERE database = currentDatabase() AND table = 'quantize_pr_framing_plain' AND active AND column = 'vec');

-- ... and the point read still returns what the ordinary read returns.
WITH (SELECT vec FROM quantize_pr_framing_adaptive WHERE id = 50) AS ref
SELECT 'adaptive_buffer_same_answer',
    (SELECT groupArray(id) FROM (SELECT id FROM quantize_pr_framing_adaptive ORDER BY L2Distance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 5))
  = (SELECT groupArray(id) FROM (SELECT id FROM quantize_pr_framing_plain     ORDER BY L2Distance(vec, ref) ASC LIMIT 10 SETTINGS vector_search_index_fetch_multiplier = 5));

DROP TABLE quantize_pr_framing_adaptive;
DROP TABLE quantize_pr_framing_plain;
