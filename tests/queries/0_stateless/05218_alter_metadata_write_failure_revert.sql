-- A COMMENT / MODIFY SETTING alter that fails while writing the metadata must not leave the
-- in-memory metadata ahead of the metadata file.

SET async_insert = 0;

DROP TABLE IF EXISTS t_revert SYNC;
DROP DICTIONARY IF EXISTS d_revert;
DROP TABLE IF EXISTS src_revert SYNC;

CREATE TABLE src_revert (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE DICTIONARY d_revert (k UInt64, v UInt64) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'src_revert')) LAYOUT(FLAT()) LIFETIME(0);

CREATE TABLE t_revert (x UInt64, y UInt64 ALIAS dictGet('d_revert', 'v', x), z UInt64 COMMENT 'c1')
ENGINE = MergeTree PARTITION BY x ORDER BY x
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;

INSERT INTO t_revert (x, z) VALUES (0, 0);

ALTER TABLE t_revert COMMENT COLUMN z 'c2';
SELECT comment FROM system.columns WHERE database = currentDatabase() AND table = 't_revert' AND name = 'z';

ALTER TABLE t_revert MODIFY SETTING min_bytes_for_wide_part = 1000000, min_rows_for_wide_part = 1000000;
INSERT INTO t_revert (x, z) VALUES (1, 1);

-- Without the dictionary the stored CREATE query no longer validates, so writing the metadata
-- fails after the fast paths already published the change in memory.
DROP DICTIONARY d_revert SETTINGS check_table_dependencies = 0;

ALTER TABLE t_revert COMMENT COLUMN z 'c3'; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_revert MODIFY SETTING min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0; -- { serverError BAD_ARGUMENTS }
INSERT INTO t_revert (x, z) VALUES (2, 2);

SELECT comment FROM system.columns WHERE database = currentDatabase() AND table = 't_revert' AND name = 'z';
SELECT create_table_query LIKE '%COMMENT \'c2\'%' FROM system.tables WHERE database = currentDatabase() AND name = 't_revert';
SELECT partition, part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_revert' AND active ORDER BY partition;

DROP TABLE t_revert SYNC;
DROP TABLE src_revert SYNC;
