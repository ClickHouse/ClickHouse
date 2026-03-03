-- Tags: no-parallel, no-parallel-replicas

-- no-parallel-replicas -- https://github.com/ClickHouse/ClickHouse/issues/90063

DROP DATABASE IF EXISTS 03710_database;
CREATE DATABASE 03710_database;

DROP TABLE IF EXISTS 03710_database.03711_join_with;
CREATE TABLE 03710_database.03711_join_with
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES 03710_database.03711_join_with;

INSERT INTO 03710_database.03711_join_with VALUES (1, 'a1'), (1, 'b1'), (1, 'c1');
INSERT INTO 03710_database.03711_join_with VALUES (2, 'a2'), (2, 'b2'), (2, 'c2');

DROP TABLE IF EXISTS 03710_database.03711_table;
CREATE TABLE 03710_database.03711_table
(
    id UInt32
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES 03710_database.03711_table;

DROP TABLE IF EXISTS 03710_database.03711_mv_table_1;
CREATE TABLE 03710_database.03711_mv_table_1
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES 03710_database.03711_mv_table_1;

DROP TABLE IF EXISTS 03710_database.03711_mv_table_2;
CREATE TABLE 03710_database.03711_mv_table_2
(
    id UInt32,
    value String
)
ENGINE = MergeTree()
ORDER BY id
SETTINGS min_bytes_for_wide_part = 10000, min_rows_for_wide_part = 10000, serialization_info_version = 'basic', string_serialization_version = 'with_size_stream';

SYSTEM STOP MERGES 03710_database.03711_mv_table_2;

DROP TABLE IF EXISTS 03710_database.03711_mv_1;
CREATE MATERIALIZED VIEW 03710_database.03711_mv_1
TO 03710_database.03711_mv_table_1 AS
SELECT r.id as id, r.value as value FROM 03710_database.03711_table as l JOIN 03710_database.03711_join_with as r ON l.id == r.id and l.id = 1;

DROP TABLE IF EXISTS 03710_database.03711_mv_2;
CREATE MATERIALIZED VIEW 03710_database.03711_mv_2
TO 03710_database.03711_mv_table_2 AS
SELECT r.id as id, r.value as value FROM 03710_database.03711_table as l JOIN 03710_database.03711_join_with as r ON l.id == r.id and l.id = 2;

SET deduplicate_blocks_in_dependent_materialized_views=1;

SET async_insert=0;
SET max_block_size=1;
SET max_insert_block_size=1;
SET min_insert_block_size_rows=0;
SET min_insert_block_size_bytes=0;

INSERT INTO 03710_database.03711_table VALUES (1), (2);

SYSTEM FLUSH LOGS part_log;

SELECT table, name, argMax(part_type, event_time_microseconds), argMax(deduplication_block_ids, event_time_microseconds) FROM system.part_log
WHERE
    table IN ['03711_join_with', '03711_table', '03711_mv_table_1', '03711_mv_table_2']
    AND database = '03710_database'
group BY database, table, name
ORDER BY ALL;

DROP DATABASE 03710_database;
