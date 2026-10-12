-- With implicit min-max indices on by default, `RENAME COLUMN c TO c_old, ADD COLUMN c` refreshes
-- the implicit index of `c` to a name derived from `c_old`, while the added column's implicit index
-- takes the old name and is shadowed by the stale on-disk index files of the renamed-away index.
-- The whole-part rewrite of a compact part used to try to build the shadowed index and fail with
-- `NOT_FOUND_COLUMN_IN_BLOCK`, leaving the mutation stuck.

-- Compact part.
DROP TABLE IF EXISTS t_rename_add_compact;
CREATE TABLE t_rename_add_compact (key Int32, value2 Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_rows_for_wide_part = 100000;
INSERT INTO t_rename_add_compact VALUES (1, 3);
ALTER TABLE t_rename_add_compact RENAME COLUMN value2 TO value2_old, ADD COLUMN value2 Int64 DEFAULT 7 SETTINGS mutations_sync = 2;
SELECT * FROM t_rename_add_compact;
CHECK TABLE t_rename_add_compact SETTINGS check_query_single_value_result = 1;
SELECT name, expr FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = 't_rename_add_compact' ORDER BY name;
DROP TABLE t_rename_add_compact;

-- Wide part: the rewritten part must not prune by the stale files of the renamed-away index.
DROP TABLE IF EXISTS t_rename_add_wide;
CREATE TABLE t_rename_add_wide (key Int32, value2 Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0, index_granularity = 1;
INSERT INTO t_rename_add_wide SELECT number, 100 + number FROM numbers(4);
ALTER TABLE t_rename_add_wide RENAME COLUMN value2 TO value2_old, ADD COLUMN value2 Int64 DEFAULT 7 SETTINGS mutations_sync = 2;
SELECT count() FROM t_rename_add_wide WHERE value2 = 7;
SELECT count() FROM t_rename_add_wide WHERE value2_old >= 100;
CHECK TABLE t_rename_add_wide SETTINGS check_query_single_value_result = 1;
DROP TABLE t_rename_add_wide;
