SET enable_alp_codec = 1;

DROP TABLE IF EXISTS t_block_value_alignment;

-- 144510 is not a multiple of 8 or 4, so a full buffer ends inside a value.
CREATE TABLE t_block_value_alignment
(
    i UInt32,
    f64 Float64 CODEC(ALP),
    n64 Nullable(Float64) CODEC(ALP)
) ENGINE = MergeTree ORDER BY i
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
    max_compress_block_size = 144510, min_compress_block_size = 3000000;

SELECT 'buffer-full flush';
INSERT INTO t_block_value_alignment SELECT number, (number + 1) / 7, if(number % 5 = 0, NULL, (number + 1) / 7) FROM numbers(50000);
SELECT count(), countIf(f64 != (i + 1) / 7), countIf(isNull(n64)), countIf(n64 != (i + 1) / 7) FROM t_block_value_alignment;

-- An adaptive buffer starts at adaptive_write_buffer_initial_size and doubles after every full flush.
SELECT 'adaptive buffer';
TRUNCATE TABLE t_block_value_alignment;
ALTER TABLE t_block_value_alignment MODIFY SETTING min_columns_to_activate_adaptive_write_buffer = 1, adaptive_write_buffer_initial_size = 100;
INSERT INTO t_block_value_alignment SELECT number, (number + 1) / 7, if(number % 5 = 0, NULL, (number + 1) / 7) FROM numbers(50000);
SELECT count(), countIf(f64 != (i + 1) / 7), countIf(isNull(n64)), countIf(n64 != (i + 1) / 7) FROM t_block_value_alignment;

-- A buffer smaller than one value holds exactly one value per compressed block.
SELECT 'buffer below one value';
TRUNCATE TABLE t_block_value_alignment;
ALTER TABLE t_block_value_alignment MODIFY SETTING max_compress_block_size = 3, min_columns_to_activate_adaptive_write_buffer = 0;
INSERT INTO t_block_value_alignment SELECT number, (number + 1) / 7, if(number % 5 = 0, NULL, (number + 1) / 7) FROM numbers(1000);
SELECT count(), countIf(f64 != (i + 1) / 7), countIf(isNull(n64)), countIf(n64 != (i + 1) / 7) FROM t_block_value_alignment;

DROP TABLE t_block_value_alignment;
