-- Tags: no-fasttest
-- The masking assertions need an engine that has a secret setting, and Kafka is an optional build.

DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS jn;
DROP TABLE IF EXISTS lg;
DROP TABLE IF EXISTS plain;
DROP TABLE IF EXISTS kfk;

CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 4096;
-- Engines that keep no settings struct of their own must still report their SETTINGS clause.
CREATE TABLE jn (a UInt64, b UInt64) ENGINE = Join(ANY, LEFT, a) SETTINGS persistent = 0;
CREATE TABLE lg (a UInt64) ENGINE = Log SETTINGS disk = 'default';
-- A table with no SETTINGS clause contributes no rows.
CREATE TABLE plain (a UInt64) ENGINE = Memory;
CREATE TABLE kfk (a String) ENGINE = Kafka
    SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 't', kafka_group_name = 'g',
             kafka_format = 'JSONEachRow', kafka_sasl_password = 'supersecret';

SELECT '-- structure';
SELECT name, type FROM system.columns WHERE database = 'system' AND table = 'table_settings' ORDER BY position;

SELECT '-- every engine reports its SETTINGS clause';
SELECT table, engine, name, value, changed, source
FROM system.table_settings
WHERE database = currentDatabase() AND table IN ('mt', 'jn', 'lg', 'plain')
ORDER BY table, name;

SELECT '-- a secret is masked, and says so';
SELECT name, value, is_masked
FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kfk' AND name = 'kafka_sasl_password';

SELECT '-- a setting that is not secret is not masked';
SELECT count()
FROM system.table_settings
WHERE database = currentDatabase() AND table = 'kfk' AND name != 'kafka_sasl_password' AND is_masked;

SELECT '-- an engine with no secret settings masks nothing';
SELECT count()
FROM system.table_settings
WHERE database = currentDatabase() AND table IN ('mt', 'jn', 'lg') AND is_masked;

SELECT '-- filtering by database reaches the scan';
SELECT count() FROM system.table_settings WHERE database = 'database_that_does_not_exist';

DROP TABLE mt;
DROP TABLE jn;
DROP TABLE lg;
DROP TABLE plain;
DROP TABLE kfk;
