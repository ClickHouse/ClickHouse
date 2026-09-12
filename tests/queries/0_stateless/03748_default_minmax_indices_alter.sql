-- { echoOn }
-- Tags: no-random-detach
-- no-random-detach: test uses DETACH/ATTACH itself
DROP TABLE IF EXISTS t_implicit;

CREATE TABLE t_implicit (a UInt64, s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 1;
SHOW CREATE TABLE t_implicit;
SELECT * FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_implicit';

ALTER TABLE t_implicit DROP COLUMN s;
SHOW CREATE TABLE t_implicit;
SELECT * FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_implicit';

ALTER TABLE t_implicit ADD COLUMN s2 String;
SHOW CREATE TABLE t_implicit;
SELECT * FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_implicit';

ALTER TABLE t_implicit ADD COLUMN a2 UInt64;
SHOW CREATE TABLE t_implicit;
SELECT * FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_implicit';

ALTER TABLE t_implicit RENAME COLUMN a2 TO a_renamed;
SHOW CREATE TABLE t_implicit;
SELECT * FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_implicit';

DETACH TABLE t_implicit;
ATTACH TABLE t_implicit;

SHOW CREATE TABLE t_implicit;
SELECT * FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_implicit';

ALTER TABLE t_implicit MODIFY COLUMN s2 UInt32;
SHOW CREATE TABLE t_implicit;
SELECT * FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_implicit';

ALTER TABLE t_implicit MODIFY COLUMN a_renamed String;
SHOW CREATE TABLE t_implicit;
SELECT * FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_implicit';

DROP TABLE t_implicit;

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

CREATE TABLE t_rename (c0 Int32, c2 Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 1;
ALTER TABLE t_rename (RENAME COLUMN c2 TO c13), (DROP COLUMN c13);
SELECT name, expr FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_rename' ORDER BY name;
DROP TABLE t_rename;

CREATE TABLE t_rename (c0 Int32, c2 Int32) ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 1;
ALTER TABLE t_rename (RENAME COLUMN c2 TO c13), (ADD COLUMN c2 Int32);
SELECT name, expr FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_rename' ORDER BY name;
DROP TABLE t_rename;

CREATE TABLE t_rename (a Int32, al Int32 ALIAS a + 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS add_minmax_index_for_numeric_columns = 1;
ALTER TABLE t_rename (MODIFY COLUMN a String), (RENAME COLUMN al TO al2), (MODIFY COLUMN a Int32);
SELECT name, expr FROM system.data_skipping_indices WHERE database = current_database() AND table = 't_rename' ORDER BY name;
DROP TABLE t_rename;