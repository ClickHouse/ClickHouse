-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: relies on alter_sync = 0 and SYSTEM SYNC REPLICA ordering of a single replica
-- no-shared-merge-tree: relies on max_replicated_mutations_in_queue
-- A mutation that applies DROP COLUMN a, RENAME COLUMN c TO a and rewrites `c` (through a patch part, or as a MATERIALIZED column) must keep `c`.

DROP TABLE IF EXISTS t_drop_rename_patch SYNC;
CREATE TABLE t_drop_rename_patch (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_drop_rename_patch', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1, enable_block_number_column = 1, enable_block_offset_column = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_drop_rename_patch VALUES (1, 10, 42, 100), (2, 20, 43, 200);
UPDATE t_drop_rename_patch SET c = 999 WHERE id = 2;
ALTER TABLE t_drop_rename_patch UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_drop_rename_patch DROP COLUMN a, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_drop_rename_patch;
ALTER TABLE t_drop_rename_patch MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_drop_rename_patch UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'drop rename patch', id, x, a FROM t_drop_rename_patch ORDER BY id;
CHECK TABLE t_drop_rename_patch SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_rename_patch;

DROP TABLE IF EXISTS t_drop_rename_patch_no_queue SYNC;
CREATE TABLE t_drop_rename_patch_no_queue (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_drop_rename_patch_no_queue', '1') ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_drop_rename_patch_no_queue VALUES (1, 10, 42, 100), (2, 20, 43, 200);
UPDATE t_drop_rename_patch_no_queue SET c = 999 WHERE id = 2;
ALTER TABLE t_drop_rename_patch_no_queue DROP COLUMN a, RENAME COLUMN c TO a SETTINGS alter_sync = 2, mutations_sync = 2;
SELECT 'drop rename patch no queue', id, x, a FROM t_drop_rename_patch_no_queue ORDER BY id;
CHECK TABLE t_drop_rename_patch_no_queue SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_rename_patch_no_queue;

DROP TABLE IF EXISTS t_drop_rename_materialized SYNC;
CREATE TABLE t_drop_rename_materialized (id UInt32, x UInt32, a UInt32, c UInt32 MATERIALIZED x + 1)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_drop_rename_materialized', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_drop_rename_materialized VALUES (1, 10, 42), (2, 20, 43);
ALTER TABLE t_drop_rename_materialized UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_drop_rename_materialized DROP COLUMN a, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_drop_rename_materialized;
ALTER TABLE t_drop_rename_materialized MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_drop_rename_materialized UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'drop rename materialized', id, x, a FROM t_drop_rename_materialized ORDER BY id;
SELECT 'drop rename materialized', column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_drop_rename_materialized' AND active AND column = 'a';
CHECK TABLE t_drop_rename_materialized SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_rename_materialized;
