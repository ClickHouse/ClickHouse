DROP TABLE IF EXISTS in_memory;
CREATE TABLE in_memory (a UInt32, b UInt32)
    ENGINE = MergeTree ORDER BY a
    SETTINGS min_rows_for_compact_part = 1000, min_rows_for_compact_part = 1000;

INSERT INTO in_memory SELECT number, number % 3 FROM numbers(100);
SELECT 'system.parts';
SELECT DISTINCT part_type, marks FROM system.parts WHERE database = currentDatabase() AND table = 'in_memory' AND active;
SELECT DISTINCT data_uncompressed_bytes > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'in_memory' AND active;
SELECT DISTINCT column_data_uncompressed_bytes > 0 FROM system.parts_columns WHERE database = currentDatabase() AND table = 'in_memory' AND active;

SELECT 'Simple selects';

SELECT * FROM in_memory ORDER BY a LIMIT 5;
SELECT * FROM in_memory ORDER BY a LIMIT 5 OFFSET 50;
SELECT count() FROM in_memory WHERE b = 0 SETTINGS max_block_size = 10;
-- Check index
SELECT count() FROM in_memory WHERE a > 100 SETTINGS max_rows_to_read = 0, force_primary_key = 1;
SELECT count() FROM in_memory WHERE a >= 10 AND a < 30 SETTINGS force_primary_key = 1;
SELECT DISTINCT blockSize() FROM in_memory SETTINGS max_block_size = 10;

SELECT 'Mutations and Alters';
SET mutations_sync = 1;

ALTER TABLE in_memory DELETE WHERE b = 0;

SELECT count() FROM in_memory;
SELECT * FROM in_memory ORDER BY a LIMIT 5;

ALTER TABLE in_memory ADD COLUMN arr Array(UInt64);
ALTER TABLE in_memory UPDATE arr = [a, a * a] WHERE b = 1;

SELECT arr FROM in_memory ORDER BY a LIMIT 5;

ALTER TABLE in_memory MODIFY COLUMN b String;
ALTER TABLE in_memory RENAME COLUMN b to str;
SELECT DISTINCT str, length(str) FROM in_memory ORDER BY str;
ALTER TABLE in_memory DROP COLUMN str;

SELECT * FROM in_memory ORDER BY a LIMIT 5;

-- in-memory parts works if they're empty.
ALTER TABLE in_memory DELETE WHERE 1;
SELECT count() FROM in_memory;

DROP TABLE in_memory;
