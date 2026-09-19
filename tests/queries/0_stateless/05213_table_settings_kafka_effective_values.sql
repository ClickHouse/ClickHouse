-- Tags: no-fasttest
-- Tag no-fasttest: needs the Kafka engine, which is an optional build.
--
-- `system.table_settings` reports the values a `Kafka` table works with, not the literals its definition states:
-- the storage expands macros in the topic list and the group name, and generates a client id when none is given.

DROP TABLE IF EXISTS kfk_effective;

CREATE TABLE kfk_effective (a String) ENGINE = Kafka
    SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = '{database}_{table}_in, {table}_other',
             kafka_group_name = '{table}_group', kafka_format = 'JSONEachRow';

SELECT '-- macros are reported expanded';
SELECT name, value = currentDatabase() || '_kfk_effective_in,kfk_effective_other' AS expanded, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kfk_effective' AND name = 'kafka_topic_list';
SELECT name, value, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kfk_effective' AND name = 'kafka_group_name';

SELECT '-- the client id the engine generates when none is given';
SELECT name, endsWith(value, '-' || currentDatabase() || '-kfk_effective') AS generated, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kfk_effective' AND name = 'kafka_client_id';

SELECT '-- no row reports the default source for a value that is not the default';
SELECT count() FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kfk_effective' AND source = 'default' AND value != `default`;

DROP TABLE kfk_effective;
