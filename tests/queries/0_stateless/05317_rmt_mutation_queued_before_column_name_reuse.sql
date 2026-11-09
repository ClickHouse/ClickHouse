-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: relies on alter_sync = 0 and SYSTEM SYNC REPLICA ordering of a single replica
-- no-shared-merge-tree: relies on max_replicated_mutations_in_queue
-- A mutation queued before an ALTER that reuses the name of a dropped or renamed column must not lose data.

DROP TABLE IF EXISTS t_readd SYNC;
CREATE TABLE t_readd (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readd', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_readd VALUES (1, 10, 42, 100), (2, 20, 43, 200);
ALTER TABLE t_readd UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_readd DROP COLUMN a, ADD COLUMN a UInt32 DEFAULT 7 SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_readd;
SELECT 'readd pending', id, x, a, c FROM t_readd ORDER BY id;
ALTER TABLE t_readd MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_readd UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'readd', id, x, a, c FROM t_readd ORDER BY id;
CHECK TABLE t_readd SETTINGS check_query_single_value_result = 1;
DROP TABLE t_readd;

DROP TABLE IF EXISTS t_drop_rename SYNC;
CREATE TABLE t_drop_rename (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_drop_rename', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_drop_rename VALUES (1, 10, 42, 100), (2, 20, 43, 200);
ALTER TABLE t_drop_rename UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_drop_rename DROP COLUMN a, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_drop_rename;
SELECT 'drop rename pending', id, x, a FROM t_drop_rename ORDER BY id;
ALTER TABLE t_drop_rename MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_drop_rename UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'drop rename', id, x, a FROM t_drop_rename ORDER BY id;
CHECK TABLE t_drop_rename SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_rename;

DROP TABLE IF EXISTS t_readd_rename SYNC;
CREATE TABLE t_readd_rename (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readd_rename', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_readd_rename VALUES (1, 10, 42, 100), (2, 20, 43, 200);
ALTER TABLE t_readd_rename UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_readd_rename DROP COLUMN a, ADD COLUMN a UInt32 DEFAULT 7, RENAME COLUMN a TO z SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_readd_rename;
SELECT 'readd rename pending', id, x, z, c FROM t_readd_rename ORDER BY id;
ALTER TABLE t_readd_rename MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_readd_rename UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'readd rename', id, x, z, c FROM t_readd_rename ORDER BY id;
CHECK TABLE t_readd_rename SETTINGS check_query_single_value_result = 1;
DROP TABLE t_readd_rename;

DROP TABLE IF EXISTS t_rename_readd_drop SYNC;
CREATE TABLE t_rename_readd_drop (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_rename_readd_drop', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_rename_readd_drop VALUES (1, 10, 42, 100), (2, 20, 43, 200);
ALTER TABLE t_rename_readd_drop UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_rename_readd_drop RENAME COLUMN a TO b, ADD COLUMN a UInt32 DEFAULT 7, DROP COLUMN a SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_rename_readd_drop;
SELECT 'rename readd drop pending', id, x, b, c FROM t_rename_readd_drop ORDER BY id;
ALTER TABLE t_rename_readd_drop MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_rename_readd_drop UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'rename readd drop', id, x, b, c FROM t_rename_readd_drop ORDER BY id;
CHECK TABLE t_rename_readd_drop SETTINGS check_query_single_value_result = 1;
DROP TABLE t_rename_readd_drop;

-- DROP COLUMN of a Nested subcolumn keeps the offsets that the other subcolumn shares.
DROP TABLE IF EXISTS t_drop_nested SYNC;
CREATE TABLE t_drop_nested (id UInt32, n Nested(a UInt32, b UInt32))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_drop_nested', '1') ORDER BY tuple()
SETTINGS share_nested_offsets = 1, min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_drop_nested VALUES (1, [1, 2], [5, 6]);
ALTER TABLE t_drop_nested DROP COLUMN `n.a` SETTINGS alter_sync = 2, mutations_sync = 2;
SELECT 'drop nested', id, n.b FROM t_drop_nested;
CHECK TABLE t_drop_nested SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_nested;
