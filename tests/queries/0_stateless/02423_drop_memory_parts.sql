-- Tags: no-s3-storage

DROP TABLE IF EXISTS table_in_memory;

CREATE TABLE table_in_memory
(
    `id` UInt64,
    `value` UInt64
)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS min_bytes_for_wide_part=1000, min_bytes_for_compact_part=900;

SELECT 'init state';
INSERT INTO table_in_memory SELECT intDiv(number, 10), number FROM numbers(30);

SELECT count() FROM table_in_memory;
SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory' AND database=currentDatabase();

SELECT 'drop part 0';
ALTER TABLE table_in_memory DROP PARTITION 0;

SELECT count() FROM table_in_memory;
SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory' AND database=currentDatabase() AND active;

SELECT 'detach table';
DETACH TABLE table_in_memory;

SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory' AND database=currentDatabase();

SELECT 'attach table';
ATTACH TABLE table_in_memory;

SELECT count() FROM table_in_memory;
SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory' AND database=currentDatabase() and active;
