set mutations_sync=1;
SET enable_analyzer = 1;
SET max_block_size = 1000000;
SET preferred_block_size_bytes = 1000000000000;
SET optimize_group_by_function_keys = 0;
SET allow_experimental_lightweight_update = 1;
SET lightweight_delete_mode = 'lightweight_update_force';
SET schema_inference_make_columns_nullable = 'auto';

drop table if exists test_updates;
CREATE TABLE test_updates (`id` UInt64, `json` JSON, `dynamic` Dynamic) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_rows_for_wide_part = 10000000, min_bytes_for_wide_part = 1000000000, allow_nullable_key=1;
INSERT INTO test_updates SELECT number, '{"a" : 42, "b" : 42, "c" : 42}', CAST('42', 'Int64') FROM numbers(1000000);
ALTER TABLE test_updates (UPDATE json = '{"a" : [1, 2, 3], "d" : 42}' WHERE id >= 500000);
ALTER TABLE test_updates (UPDATE json = '{"a" : [1, 2, 3], "d" : 42}' WHERE id <= 500000);
ALTER TABLE test_updates (UPDATE json = '{"a" : "a", "d" : 41}' WHERE id <= 500000);
ALTER TABLE test_updates (UPDATE json = '{"a" : "a", "d" : 41}' WHERE id >= 500000);
DROP TABLE test_updates;
