-- { echoOn }
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