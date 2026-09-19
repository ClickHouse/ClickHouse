-- Tags: no-fasttest
-- Tag no-fasttest: needs the Kafka engine, which is an optional build.
--
-- `source = 'default'` has to mean the value is the default. Two ways it did not:
-- - a `Distributed` table copies the server's `distributed_background_insert_*` settings into the ones its
--   definition does not state, and copying a `Milliseconds` field copies its changed bit too, so the value
--   changed while the setting still read as unchanged;
-- - a secret setting nobody set was masked, so an empty password reported `[HIDDEN]`.

DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS dist_src;
DROP TABLE IF EXISTS kfk_no_password;

CREATE TABLE dist_src (a UInt64) ENGINE = Memory;
CREATE TABLE dist AS dist_src ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'dist_src');
CREATE TABLE kfk_no_password (a String) ENGINE = Kafka
    SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 't', kafka_group_name = 'g', kafka_format = 'JSONEachRow';

SELECT '-- no row reports the default source for a value that is not the default';
SELECT table, name, value, `default`, source FROM system.table_settings
WHERE database = currentDatabase() AND source = 'default' AND value != `default`
ORDER BY table, name;

SELECT '-- the settings a Distributed table copies from the server are attributed';
SELECT name, value != `default` AS copied, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'dist' AND alias_for = ''
  AND name IN ('background_insert_sleep_time_ms', 'background_insert_max_sleep_time_ms')
ORDER BY name;

SELECT '-- an unset secret is not masked';
SELECT name, value, is_masked, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kfk_no_password' AND name = 'kafka_sasl_password';

DROP TABLE dist;
DROP TABLE dist_src;
DROP TABLE kfk_no_password;
