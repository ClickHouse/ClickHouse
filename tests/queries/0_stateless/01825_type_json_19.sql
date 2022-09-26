-- Tags: no-fasttest

SET allow_experimental_object_type = 1;

DROP TABLE IF EXISTS table_in_memory_with_json;

CREATE TABLE table_in_memory_with_json
(
    id UInt64,
    value UInt64,
    data Object('JSON')
)
ENGINE = MergeTree
PARTITION BY id
ORDER BY value
SETTINGS min_bytes_for_wide_part=1000, min_bytes_for_compact_part=900;

SELECT 'init state';
INSERT INTO table_in_memory_with_json
FORMAT JSONEachRow {"id": 0, "value": 1, "data" : {"k1": 1}}, {"id": 0, "value": 2, "data" : {"k1": 2}}, {"id": 1, "value": 3, "data" : {"k1": 3}}, {"id": 1, "value": 4, "data" : {"k1": 4}}, {"id": 2, "value": 5, "data" : {"k1": 5}}, {"id": 2, "value": 6, "data" : {"k1": 6}};
SELECT id, data, toTypeName(data) FROM table_in_memory_with_json ORDER BY id;

SELECT count() FROM table_in_memory_with_json;
SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory_with_json' AND database=currentDatabase();

SELECT 'drop part 0';
ALTER TABLE table_in_memory_with_json DROP PARTITION 0;

SELECT count() FROM table_in_memory_with_json;
SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory_with_json' AND database=currentDatabase();

SELECT 'detach table';
DETACH TABLE table_in_memory_with_json;

SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory_with_json' AND database=currentDatabase();

SELECT 'attach table';
ATTACH TABLE table_in_memory_with_json;

SELECT count() FROM table_in_memory;
SELECT name, part_type, rows, active from system.parts
WHERE table='table_in_memory_with_json' AND database=currentDatabase();


