-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: reads system.parts_columns, which randomized enable_block_number/offset_column add rows to.

-- Adaptive selection picks a codec per block, so one column's stream can mix codecs. Reading it must work.
DROP TABLE IF EXISTS t_adaptive_mixed;

CREATE TABLE t_adaptive_mixed
(
    id UInt64,
    x UInt64
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, allow_experimental_adaptive_codec_selection = 1, max_compress_block_size = 65536;

-- Two parts so OPTIMIZE FINAL merges (adaptive runs only on merge). In x, low ids are random (LZ4), high ids monotonic (T64)
INSERT INTO t_adaptive_mixed SELECT number, if(number < 500000, cityHash64(number), number) FROM numbers(1000000);
INSERT INTO t_adaptive_mixed SELECT number, number FROM numbers(1000000, 1000000);
OPTIMIZE TABLE t_adaptive_mixed FINAL;

-- Sanity: x really has >1 codec, else the read below proves nothing.
SELECT 'mixed codecs', length(codec_block_counts) > 1
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_adaptive_mixed' AND active AND column = 'x';

SELECT 'read', sum(x), count() FROM t_adaptive_mixed;

CHECK TABLE t_adaptive_mixed SETTINGS check_query_single_value_result = 1;

DROP TABLE t_adaptive_mixed;
