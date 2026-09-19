-- Tags: no-fasttest
-- Tag no-fasttest: needs the Kafka engine, which is an optional build.
--
-- `Kafka` records the table's own `SETTINGS` clause in the settings object as it applies it, so
-- `system.table_settings` reads the source from there rather than from the stored `CREATE` query. The two have
-- to agree on `CREATE` and after the table is loaded again from what it stored.
--
-- Except where the engine replaces what the clause stated: with `kafka_handle_error_mode = 'stream'` the
-- constructor pins `input_format_allow_errors_num` and `input_format_allow_errors_ratio` to 0, and the row
-- names the engine, not a clause that states another value - as `05229` pins for a named collection.

DROP TABLE IF EXISTS kafka_definition;
DROP TABLE IF EXISTS kafka_definition_pinned;

CREATE TABLE kafka_definition (a String) ENGINE = Kafka
    SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 't', kafka_group_name = 'g',
             kafka_format = 'JSONEachRow', kafka_max_block_size = 4242, input_format_allow_errors_num = 5;

CREATE TABLE kafka_definition_pinned (a String) ENGINE = Kafka
    SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 't', kafka_group_name = 'g',
             kafka_format = 'JSONEachRow', kafka_handle_error_mode = 'stream', input_format_allow_errors_num = 5;

SELECT '-- CREATE';
SELECT table, name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table LIKE 'kafka_definition%'
    AND name IN ('kafka_max_block_size', 'input_format_allow_errors_num', 'kafka_num_consumers')
ORDER BY table, name;

DETACH TABLE kafka_definition;
ATTACH TABLE kafka_definition;
DETACH TABLE kafka_definition_pinned;
ATTACH TABLE kafka_definition_pinned;

SELECT '-- loaded again from what they stored';
SELECT table, name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table LIKE 'kafka_definition%'
    AND name IN ('kafka_max_block_size', 'input_format_allow_errors_num', 'kafka_num_consumers')
ORDER BY table, name;

DROP TABLE kafka_definition;
DROP TABLE kafka_definition_pinned;
