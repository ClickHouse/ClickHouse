-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree
-- no-replicated-database: relies on alter_sync = 0 and SYSTEM SYNC REPLICA ordering of a single replica
-- no-shared-merge-tree: relies on max_replicated_mutations_in_queue
-- A mutation queued before an ALTER that reuses a column name must keep the serialization infos and statistics of the part consistent.

-- DROP COLUMN a, ADD COLUMN a with `a` stored as a missing-column marker.
DROP TABLE IF EXISTS t_readd_marker SYNC;
CREATE TABLE t_readd_marker (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_readd_marker', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1,
    skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_readd_marker VALUES (1, 10, 0, 100), (2, 20, 0, 200);
ALTER TABLE t_readd_marker UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_readd_marker DROP COLUMN a, ADD COLUMN a UInt32 DEFAULT 7 SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_readd_marker;
ALTER TABLE t_readd_marker MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_readd_marker UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'readd marker', id, x, a, c FROM t_readd_marker ORDER BY id;
CHECK TABLE t_readd_marker SETTINGS check_query_single_value_result = 1;
DROP TABLE t_readd_marker;

-- A column stored as a missing-column marker renamed onto the dropped name, and a sparse one.
DROP TABLE IF EXISTS t_drop_rename_marker SYNC;
CREATE TABLE t_drop_rename_marker (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_drop_rename_marker', '1') ORDER BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1,
    skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_drop_rename_marker VALUES (1, 10, 42, 0), (2, 20, 43, 0);
ALTER TABLE t_drop_rename_marker UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_drop_rename_marker DROP COLUMN a, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_drop_rename_marker;
ALTER TABLE t_drop_rename_marker MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_drop_rename_marker UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'drop rename marker', id, x, a FROM t_drop_rename_marker ORDER BY id;
CHECK TABLE t_drop_rename_marker SETTINGS check_query_single_value_result = 1;
DROP TABLE t_drop_rename_marker;

DROP TABLE IF EXISTS t_drop_rename_sparse SYNC;
CREATE TABLE t_drop_rename_sparse (id UInt32, x UInt32, a UInt32, c UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_drop_rename_sparse', '1') ORDER BY id
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1, ratio_of_defaults_for_sparse_serialization = 0.5,
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_drop_rename_sparse SELECT number, number, 42, if(number % 100 = 0, number, 0) FROM numbers(1000);
ALTER TABLE t_drop_rename_sparse UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_drop_rename_sparse DROP COLUMN a, RENAME COLUMN c TO a SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_drop_rename_sparse;
ALTER TABLE t_drop_rename_sparse MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_drop_rename_sparse UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'drop rename sparse', sum(a), countIf(a != 0), sum(x) FROM t_drop_rename_sparse;
SELECT 'drop rename sparse', column, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_drop_rename_sparse' AND active AND column = 'a';
DROP TABLE t_drop_rename_sparse;

-- CLEAR STATISTICS names a column as it is before a rename or a drop of the same ALTER.
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_clear_rename SYNC;
CREATE TABLE t_clear_rename (x UInt64, a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_rename', '1') ORDER BY tuple() PARTITION BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1, auto_statistics_types = 'basic',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_clear_rename VALUES (1, 2, 3);
ALTER TABLE t_clear_rename UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_rename CLEAR STATISTICS a IN PARTITION tuple(), RENAME COLUMN a TO a1 SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_clear_rename;
ALTER TABLE t_clear_rename MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_clear_rename UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'clear rename', column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_clear_rename' AND active AND NOT startsWith(column, '_') ORDER BY column;
DROP TABLE t_clear_rename;

DROP TABLE IF EXISTS t_clear_drop_rename SYNC;
CREATE TABLE t_clear_drop_rename (x UInt64, a UInt64 STATISTICS(tdigest), b UInt64 STATISTICS(tdigest))
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_clear_drop_rename', '1') ORDER BY tuple() PARTITION BY tuple()
SETTINGS max_replicated_mutations_in_queue = 0, replicated_max_mutations_in_one_entry = 1, auto_statistics_types = 'basic',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0, merge_selecting_sleep_ms = 100, max_merge_selecting_sleep_ms = 200;

INSERT INTO t_clear_drop_rename VALUES (1, 2, 3);
ALTER TABLE t_clear_drop_rename UPDATE x = x + 1 WHERE 1 SETTINGS mutations_sync = 0;
ALTER TABLE t_clear_drop_rename CLEAR STATISTICS a IN PARTITION tuple(), DROP COLUMN a, RENAME COLUMN b TO a SETTINGS alter_sync = 0;
SYSTEM SYNC REPLICA t_clear_drop_rename;
ALTER TABLE t_clear_drop_rename MODIFY SETTING max_replicated_mutations_in_queue = 16;
ALTER TABLE t_clear_drop_rename UPDATE x = x WHERE 1 SETTINGS mutations_sync = 2;
SELECT 'clear drop rename', column, statistics FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_clear_drop_rename' AND active AND NOT startsWith(column, '_') ORDER BY column;
DROP TABLE t_clear_drop_rename;
