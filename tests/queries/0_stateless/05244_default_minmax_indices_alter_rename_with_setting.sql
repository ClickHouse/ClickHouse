-- { echoOn }
-- Tags: no-replicated-database
-- Tag no-replicated-database: `ALTER`s of replicated and non-replicated types cannot be mixed in one query
-- The cases of 03748_default_minmax_indices_alter.sql where a `RENAME COLUMN` shares one `ALTER` with a
-- `MODIFY SETTING`, split out so that the rest of that test keeps running on `Replicated` databases.
DROP TABLE IF EXISTS t_rename;

CREATE TABLE t_rename (c0 Int32, c2 Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 1;
ALTER TABLE t_rename (RENAME COLUMN c2 TO c13), (MODIFY SETTING fsync_part_directory = 1);
SELECT name, expr FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_rename' ORDER BY name;
DROP TABLE t_rename;

CREATE TABLE t_rename (c0 Int32, s String, d Date) ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_string_columns = 1, add_minmax_index_for_temporal_columns = 1;
ALTER TABLE t_rename (RENAME COLUMN s TO s2), (MODIFY SETTING fsync_part_directory = 1);
ALTER TABLE t_rename (RENAME COLUMN d TO d2), (MODIFY SETTING fsync_part_directory = 0);
SELECT name, expr FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_rename' ORDER BY name;
DROP TABLE t_rename;

CREATE TABLE t_rename (value Int32, al Int32 ALIAS value > 0) ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 1;
ALTER TABLE t_rename (RENAME COLUMN al TO al2), (MODIFY SETTING fsync_part_directory = 1);
SELECT name, expr FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_rename' ORDER BY name;
DROP TABLE t_rename;
