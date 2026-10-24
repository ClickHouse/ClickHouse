-- Recompressing a Wide-part column must resolve the source stream's on-disk file name from the part's
-- own recorded files, not recompute it from the table's *current* `replace_long_file_name_to_hash` /
-- `max_file_name_length`. When those settings change after the part is written, a name recomputed from
-- the current settings no longer matches the file on disk, so the stream would be treated as absent and
-- the recompression would silently do nothing.

SET mutations_sync = 2;
SET check_query_single_value_result = 1;

DROP TABLE IF EXISTS t_recompress_hash;

-- `max_file_name_length = 32` makes `payload_column_with_a_long_enough_name` a "long" stream name;
-- with `replace_long_file_name_to_hash = 0` it is stored on disk under its plain (escaped) name.
CREATE TABLE t_recompress_hash (id UInt64, payload_column_with_a_long_enough_name String CODEC(NONE))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, replace_long_file_name_to_hash = 0, max_file_name_length = 32;

INSERT INTO t_recompress_hash SELECT number, repeat('a', 100) FROM numbers(100000);

SELECT DISTINCT 'wide part', part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_recompress_hash' AND active;
SELECT 'none is large', sum(data_compressed_bytes) > 5000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_hash' AND column = 'payload_column_with_a_long_enough_name' AND active;

-- Change the codec (metadata-only) and flip the on-disk-name policy, so a name recomputed from the
-- current settings would be the hash, not the plain name actually on disk.
ALTER TABLE t_recompress_hash MODIFY COLUMN payload_column_with_a_long_enough_name CODEC(ZSTD);
ALTER TABLE t_recompress_hash MODIFY SETTING replace_long_file_name_to_hash = 1;
ALTER TABLE t_recompress_hash RECOMPRESS COLUMN payload_column_with_a_long_enough_name;

SELECT 'after', count(), countIf(payload_column_with_a_long_enough_name = repeat('a', 100)), countIf(id < 100000) FROM t_recompress_hash;
SELECT 'zstd is small', sum(data_compressed_bytes) < 1000000 FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_recompress_hash' AND column = 'payload_column_with_a_long_enough_name' AND active;

SELECT 'point', payload_column_with_a_long_enough_name = repeat('a', 100) FROM t_recompress_hash WHERE id = 99999;

CHECK TABLE t_recompress_hash;

DROP TABLE t_recompress_hash;
