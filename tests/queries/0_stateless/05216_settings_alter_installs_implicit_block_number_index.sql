-- Tags: no-random-merge-tree-settings

-- A settings-only ALTER that changes the set of implicit skip indices must install the recomputed
-- metadata into the running table, not only persist it. Before the fix, the table below only got
-- `auto_minmax_index__block_number` after DETACH / ATTACH or restart, and parts merged in between
-- were built without the index files. Level-0 parts never carry this index (the block number is
-- provisional at insert), so the probe is the merged part.

DROP TABLE IF EXISTS t_settings_alter_implicit_index;

CREATE TABLE t_settings_alter_implicit_index (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS add_minmax_index_for_block_number_column = 1, add_minmax_index_for_numeric_columns = 0;

INSERT INTO t_settings_alter_implicit_index SELECT number, number FROM numbers(10);

SELECT 'before', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_settings_alter_implicit_index' ORDER BY name;

ALTER TABLE t_settings_alter_implicit_index MODIFY SETTING enable_block_number_column = 1;

SELECT 'live after enabling', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_settings_alter_implicit_index' ORDER BY name;

INSERT INTO t_settings_alter_implicit_index SELECT number, number FROM numbers(10);
OPTIMIZE TABLE t_settings_alter_implicit_index FINAL;

SELECT 'merged part has the index', name, secondary_indices_marks_bytes > 0 FROM system.parts
WHERE database = currentDatabase() AND table = 't_settings_alter_implicit_index' AND active ORDER BY name;

DETACH TABLE t_settings_alter_implicit_index;
ATTACH TABLE t_settings_alter_implicit_index;

SELECT 'after reattach', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_settings_alter_implicit_index' ORDER BY name;

ALTER TABLE t_settings_alter_implicit_index MODIFY SETTING enable_block_number_column = 0;

SELECT 'live after disabling', count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_settings_alter_implicit_index';

DROP TABLE t_settings_alter_implicit_index;
