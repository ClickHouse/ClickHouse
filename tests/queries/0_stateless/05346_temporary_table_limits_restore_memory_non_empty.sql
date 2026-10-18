-- `RESTORE` into a non-empty temporary `Memory` table counts the data already in the table against `max_temporary_table_memory_usage`.

DROP TABLE IF EXISTS tmp_memory;
CREATE TEMPORARY TABLE tmp_memory (x UInt64) ENGINE = Memory;
-- About 800 KB, so the table and the backup fit under 1 MiB each, but not together.
INSERT INTO tmp_memory SELECT number FROM numbers(100000) SETTINGS max_block_size = 1000000, max_insert_block_size = 1000000;
BACKUP TEMPORARY TABLE tmp_memory TO Memory('b') FORMAT Null;
RESTORE TEMPORARY TABLE tmp_memory FROM Memory('b') SETTINGS allow_non_empty_tables = 1, max_temporary_table_memory_usage = '1Mi' FORMAT Null; -- { serverError TOO_MANY_BYTES }
SELECT count() FROM tmp_memory;
TRUNCATE TABLE tmp_memory;
RESTORE TEMPORARY TABLE tmp_memory FROM Memory('b') SETTINGS allow_non_empty_tables = 1, max_temporary_table_memory_usage = '1Mi' FORMAT Null;
SELECT count() FROM tmp_memory;
DROP TEMPORARY TABLE tmp_memory;
DROP TABLE IF EXISTS tmp_memory;
