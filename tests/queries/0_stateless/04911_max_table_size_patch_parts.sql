-- Patch parts from lightweight mutations do not count towards max_table_size_rows.

DROP TABLE IF EXISTS t_max_size_patch_parts;

CREATE TABLE t_max_size_patch_parts (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_table_size_rows = 10, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_max_size_patch_parts SELECT number, number FROM numbers(10);

DELETE FROM t_max_size_patch_parts WHERE x = 0 SETTINGS lightweight_delete_mode = 'lightweight_update_force', lightweight_deletes_sync = 2;
INSERT INTO t_max_size_patch_parts VALUES (10, 10);
SELECT count() FROM t_max_size_patch_parts;
INSERT INTO t_max_size_patch_parts VALUES (11, 11); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

DROP TABLE t_max_size_patch_parts;

CREATE TABLE t_max_size_patch_parts (x UInt64, y UInt64) ENGINE = MergeTree ORDER BY x SETTINGS max_table_size_rows = 10, enable_block_number_column = 1, enable_block_offset_column = 1;
INSERT INTO t_max_size_patch_parts SELECT number, number FROM numbers(10);

UPDATE t_max_size_patch_parts SET y = y + 100 WHERE x = 0 SETTINGS mutations_sync = 2;
INSERT INTO t_max_size_patch_parts VALUES (10, 10);
SELECT count() FROM t_max_size_patch_parts;
INSERT INTO t_max_size_patch_parts VALUES (11, 11); -- { serverError TABLE_SIZE_LIMIT_EXCEEDED }

DROP TABLE t_max_size_patch_parts;
