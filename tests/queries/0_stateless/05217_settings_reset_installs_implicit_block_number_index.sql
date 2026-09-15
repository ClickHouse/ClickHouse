-- Tags: no-random-merge-tree-settings

-- Reset-style settings changes are policy inputs for the implicit skip indices too. Before the fix,
-- `MODIFY SETTING enable_block_number_column = DEFAULT` (parsed as a reset) and
-- `RESET SETTING enable_block_number_column` removed the stored override but never recomputed
-- `add_minmax_index_for_block_number_column` in the metadata, so the running table kept
-- `auto_minmax_index__block_number` until DETACH / ATTACH or restart, while the persisted
-- definition already said otherwise. Level-0 parts never carry this index (the block number is
-- provisional at insert), so the part-level probe is the merged part.

DROP TABLE IF EXISTS t_settings_reset_implicit_index;

CREATE TABLE t_settings_reset_implicit_index (id UInt64, v UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS add_minmax_index_for_block_number_column = 1, enable_block_number_column = 1, add_minmax_index_for_numeric_columns = 0;

SELECT 'created', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_settings_reset_implicit_index' ORDER BY name;

ALTER TABLE t_settings_reset_implicit_index MODIFY SETTING enable_block_number_column = DEFAULT;

SELECT 'live after = DEFAULT', count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_settings_reset_implicit_index';

INSERT INTO t_settings_reset_implicit_index SELECT number, number FROM numbers(10);
INSERT INTO t_settings_reset_implicit_index SELECT number, number FROM numbers(10);
OPTIMIZE TABLE t_settings_reset_implicit_index FINAL;

SELECT 'merged part without the index', name, secondary_indices_marks_bytes FROM system.parts
WHERE database = currentDatabase() AND table = 't_settings_reset_implicit_index' AND active ORDER BY name;

DETACH TABLE t_settings_reset_implicit_index;
ATTACH TABLE t_settings_reset_implicit_index;

SELECT 'after reattach', count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_settings_reset_implicit_index';

ALTER TABLE t_settings_reset_implicit_index MODIFY SETTING enable_block_number_column = 1;

SELECT 'live after enabling again', name FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_settings_reset_implicit_index' ORDER BY name;

ALTER TABLE t_settings_reset_implicit_index RESET SETTING enable_block_number_column;

SELECT 'live after RESET SETTING', count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_settings_reset_implicit_index';

DETACH TABLE t_settings_reset_implicit_index;
ATTACH TABLE t_settings_reset_implicit_index;

SELECT 'after reattach again', count() FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_settings_reset_implicit_index';

DROP TABLE t_settings_reset_implicit_index;
