-- Reads a table carrying a patch part in packed part storage. Most of these settings are
-- randomized per run (default_compression_codec server-side, by tests/config/install.sh) and the
-- shape needs every one of them, so they are all pinned here.
-- Do not add no-random-settings or no-random-merge-tree-settings: those tags also pin
-- default_compression_codec back to LZ4, which silences the shape this test covers.

DROP TABLE IF EXISTS t_lwu_packed_bound;

CREATE TABLE t_lwu_packed_bound (id UInt64, value String)
ENGINE = MergeTree ORDER BY id
SETTINGS
    index_granularity = 8192,
    index_granularity_bytes = '10Mi',
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 536870912,
    min_columns_to_activate_adaptive_write_buffer = 2,
    adaptive_write_buffer_initial_size = 16384,
    default_compression_codec = 'ZSTD(3)',
    enable_block_number_column = 1,
    enable_block_offset_column = 1;

INSERT INTO t_lwu_packed_bound SELECT number, randomString(10) FROM numbers(200000);

SET enable_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';
SET mutations_sync = 2;

DELETE FROM t_lwu_packed_bound WHERE id < 100000;

-- The read below reaches ReadBufferFromFileView only while the patch part is Packed, and it returns
-- the same row under Full storage, so assert the storage type rather than assuming it.
SELECT count(), part_type, part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwu_packed_bound' AND active AND startsWith(name, 'patch')
GROUP BY part_type, part_storage_type ORDER BY part_type, part_storage_type;

SELECT id, length(value) FROM t_lwu_packed_bound ORDER BY id LIMIT 1
SETTINGS optimize_read_in_order = 1, max_block_size = 65409;

DROP TABLE t_lwu_packed_bound;
