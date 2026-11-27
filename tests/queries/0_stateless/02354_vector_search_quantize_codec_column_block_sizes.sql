-- For a column with a `Quantized` codec, the column-level min/max_compress_block_size apply only to its vectors
-- (the array elements); its array sizes and quantized codes use the table-level values.

SET enable_quantized_codec = 1;

DROP TABLE IF EXISTS tab_small;
DROP TABLE IF EXISTS tab_big;

-- Table-level blocks are large; the column-level ones are small.
CREATE TABLE tab_small
(
    id UInt64,
    v_min Array(Float32) CODEC(Quantized('rabitq', 64)) SETTINGS (min_compress_block_size = 256),
    v_max Array(Float32) CODEC(Quantized('rabitq', 64)) SETTINGS (max_compress_block_size = 1024),
    plain Array(Float32) SETTINGS (min_compress_block_size = 256),
    `n.a` Array(Float32) SETTINGS (min_compress_block_size = 256),
    `n.b` Array(Float32) CODEC(Quantized('rabitq', 64)),
    `m.b` Array(Float32) CODEC(Quantized('rabitq', 64)) SETTINGS (min_compress_block_size = 256),
    `m.a` Array(Float32)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 64, index_granularity_bytes = '10Mi',
    min_compress_block_size = 65536, max_compress_block_size = 1048576,
    min_columns_to_activate_adaptive_write_buffer = 0, max_memory_ratio_to_activate_adaptive_write_buffer = 0,
    enable_vertical_merge_algorithm = 1, vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0;

-- Table-level blocks are small; the column-level ones are large.
CREATE TABLE tab_big
(
    id UInt64,
    v_big Array(Float32) CODEC(Quantized('rabitq', 64)) SETTINGS (min_compress_block_size = 4194304, max_compress_block_size = 4194304)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, index_granularity = 64, index_granularity_bytes = '10Mi',
    min_compress_block_size = 256, max_compress_block_size = 1048576,
    min_columns_to_activate_adaptive_write_buffer = 0, max_memory_ratio_to_activate_adaptive_write_buffer = 0,
    enable_vertical_merge_algorithm = 0;

INSERT INTO tab_small SELECT number, v, v, v, v, v, v, v FROM (SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)) AS v FROM numbers(4096))
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
INSERT INTO tab_big SELECT number, arrayMap(j -> toFloat32(sipHash64(number, j) % 2000 / 1000.0 - 1.0), range(64)) FROM numbers(4096)
SETTINGS max_insert_threads = 1, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;

-- The merge rewrites the part: vertical for tab_small, horizontal for tab_big.
OPTIMIZE TABLE tab_small FINAL;
OPTIMIZE TABLE tab_big FINAL;

SELECT table, count() FROM system.parts WHERE database = currentDatabase() AND table IN ('tab_small', 'tab_big') AND active GROUP BY table ORDER BY table;

-- Compressed blocks the data granules start in, per stream: vectors, sizes, codes, granules.
SELECT 'v_min', uniqExact((`v_min.mark`).offset_in_compressed_file), uniqExact((`v_min.size0.mark`).offset_in_compressed_file),
    uniqExact((`v_min.quantized.mark`).offset_in_compressed_file), count()
FROM mergeTreeIndex(currentDatabase(), tab_small, with_marks = true) WHERE rows_in_granule > 0;
SELECT 'v_max', uniqExact((`v_max.mark`).offset_in_compressed_file), uniqExact((`v_max.size0.mark`).offset_in_compressed_file),
    uniqExact((`v_max.quantized.mark`).offset_in_compressed_file), count()
FROM mergeTreeIndex(currentDatabase(), tab_small, with_marks = true) WHERE rows_in_granule > 0;
SELECT 'plain', uniqExact((`plain.mark`).offset_in_compressed_file), uniqExact((`plain.size0.mark`).offset_in_compressed_file), count()
FROM mergeTreeIndex(currentDatabase(), tab_small, with_marks = true) WHERE rows_in_granule > 0;
SELECT 'v_big', uniqExact((`v_big.mark`).offset_in_compressed_file), uniqExact((`v_big.size0.mark`).offset_in_compressed_file),
    uniqExact((`v_big.quantized.mark`).offset_in_compressed_file), count()
FROM mergeTreeIndex(currentDatabase(), tab_big, with_marks = true) WHERE rows_in_granule > 0;

-- Array sizes shared within a `Nested` structure follow the column that writes them first: plain `n.a`, `Quantized` `m.b`.
SELECT 'nested', uniqExact((`n.size0.mark`).offset_in_compressed_file), uniqExact((`m.size0.mark`).offset_in_compressed_file), count()
FROM mergeTreeIndex(currentDatabase(), tab_small, with_marks = true) WHERE rows_in_granule > 0;

-- The streams read back the same values whatever their block sizes.
SELECT (SELECT sum(cityHash64(v_min, v_min.quantized, v_min.size0)) FROM tab_small)
     = (SELECT sum(cityHash64(v_big, v_big.quantized, v_big.size0)) FROM tab_big),
       (SELECT sum(cityHash64(v_min, v_min.quantized)) = sum(cityHash64(v_max, v_max.quantized)) FROM tab_small);

DROP TABLE tab_small;
DROP TABLE tab_big;
