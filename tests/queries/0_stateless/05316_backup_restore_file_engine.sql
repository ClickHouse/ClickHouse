-- BACKUP of a `File` table that keeps its data in the table's data path must store the data file, and RESTORE must write it back.
-- https://github.com/ClickHouse/ClickHouse/issues/122289

DROP TABLE IF EXISTS f;
DROP TABLE IF EXISTS f_copy;
DROP TABLE IF EXISTS f_multi;

CREATE TABLE f (x UInt64) ENGINE = File(CSV);
INSERT INTO f SELECT number FROM numbers(50);
BACKUP TABLE f TO Memory('f_b') FORMAT Null;

RESTORE TABLE f AS f_copy FROM Memory('f_b') FORMAT Null;
SELECT 'copy', count(), sum(x) FROM f_copy;

DROP TABLE f SYNC;
RESTORE TABLE f FROM Memory('f_b') FORMAT Null;
SELECT 'restored', count(), sum(x) FROM f;

-- A non-empty table is refused unless allow_non_empty_tables is set, which adds the backup's file next to the existing one.
RESTORE TABLE f FROM Memory('f_b') FORMAT Null; -- { serverError CANNOT_RESTORE_TABLE }
RESTORE TABLE f FROM Memory('f_b') SETTINGS allow_non_empty_tables = 1 FORMAT Null;
SELECT 'non-empty', count(), sum(x) FROM f;

-- A format that cannot be appended to gets a new file per insert: every file goes to the backup.
CREATE TABLE f_multi (x UInt64) ENGINE = File(JSON);
INSERT INTO f_multi SELECT number FROM numbers(3) SETTINGS engine_file_allow_create_multiple_files = 1;
INSERT INTO f_multi SELECT number FROM numbers(3, 3) SETTINGS engine_file_allow_create_multiple_files = 1;
BACKUP TABLE f_multi TO Memory('f_multi_b') FORMAT Null;
DROP TABLE f_multi SYNC;
RESTORE TABLE f_multi FROM Memory('f_multi_b') FORMAT Null;
SELECT 'multiple files', _file, count(), sum(x) FROM f_multi GROUP BY _file ORDER BY _file;

DROP TABLE f;
DROP TABLE f_copy;
DROP TABLE f_multi;
