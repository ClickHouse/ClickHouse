-- BACKUP of Set and Join tables must store their data, and RESTORE must load it back.
-- https://github.com/ClickHouse/ClickHouse/issues/121176

DROP TABLE IF EXISTS set_src;
DROP TABLE IF EXISTS set_copy;
DROP TABLE IF EXISTS set_np;
DROP TABLE IF EXISTS join_src;

-- Two inserts make two data files (1.bin, 2.bin) with overlapping keys.
CREATE TABLE set_src (k UInt64) ENGINE = Set;
INSERT INTO set_src SELECT number FROM numbers(100);
INSERT INTO set_src SELECT number FROM numbers(50, 100);
SELECT 'source', count() FROM numbers(300) WHERE number IN set_src;

BACKUP TABLE set_src TO Memory('set_b') FORMAT Null;
DROP TABLE set_src SYNC;
RESTORE TABLE set_src FROM Memory('set_b') FORMAT Null;
SELECT 'restored', count() FROM numbers(300) WHERE number IN set_src;

-- Restore under another name, insert more, and reload from disk: the restored files persist and new inserts follow them.
RESTORE TABLE set_src AS set_copy FROM Memory('set_b') FORMAT Null;
INSERT INTO set_copy SELECT number FROM numbers(200, 10);
DETACH TABLE set_copy;
ATTACH TABLE set_copy;
SELECT 'reattached', count() FROM numbers(300) WHERE number IN set_copy;

-- A table with persistent = 0 loads the data but writes no files, so it is empty again after a reload.
CREATE TABLE set_np (k UInt64) ENGINE = Set SETTINGS persistent = 0;
RESTORE TABLE set_src AS set_np FROM Memory('set_b') SETTINGS allow_different_table_def = 1 FORMAT Null;
SELECT 'not persistent', count() FROM numbers(300) WHERE number IN set_np;
DETACH TABLE set_np;
ATTACH TABLE set_np;
SELECT 'not persistent reattached', count() FROM numbers(300) WHERE number IN set_np;

-- A non-empty table is refused unless allow_non_empty_tables is set.
RESTORE TABLE set_src FROM Memory('set_b') FORMAT Null; -- { serverError CANNOT_RESTORE_TABLE }
RESTORE TABLE set_src FROM Memory('set_b') SETTINGS allow_non_empty_tables = 1 FORMAT Null;
SELECT 'non-empty', count() FROM numbers(300) WHERE number IN set_src;

-- Join shares the storage code with Set.
CREATE TABLE join_src (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO join_src VALUES (1, 'a'), (2, 'b');
INSERT INTO join_src VALUES (3, 'c');
BACKUP TABLE join_src TO Memory('join_b') FORMAT Null;
DROP TABLE join_src SYNC;
RESTORE TABLE join_src FROM Memory('join_b') FORMAT Null;
SELECT 'join', joinGet(join_src, 'v', toUInt64(1)), joinGet(join_src, 'v', toUInt64(3));

DROP TABLE set_src;
DROP TABLE set_copy;
DROP TABLE set_np;
DROP TABLE join_src;
