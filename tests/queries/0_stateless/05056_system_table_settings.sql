-- Tags: no-fasttest
-- The masking assertions need an engine that has a secret setting, and Kafka is an optional build.

DROP TABLE IF EXISTS mt;
DROP TABLE IF EXISTS jn;
DROP TABLE IF EXISTS lg;
DROP TABLE IF EXISTS plain;
DROP TABLE IF EXISTS kfk;

CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a
    SETTINGS index_granularity = 4096, enable_block_number_column = 1;
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

SELECT '-- every engine reports what its definition states';
-- Only the settings these definitions name. Not every row whose source is `definition`: the test
-- harness randomizes MergeTree settings into the `CREATE` query, so that set is whatever it chose
-- this run. `plain` names none and so contributes nothing, which is the point of listing it.
SELECT table, engine, name, value, changed, source
FROM system.table_settings
WHERE database = currentDatabase() AND table IN ('mt', 'jn', 'lg', 'plain')
  AND source = 'definition'
  AND name IN ('index_granularity', 'enable_block_number_column', 'persistent', 'disk')
ORDER BY table, name;

SELECT '-- an engine with a settings struct reports its defaults too, one without does not';
SELECT table, countIf(source = 'default') > 0 AS has_defaults
FROM system.table_settings
WHERE database = currentDatabase() AND table IN ('mt', 'jn', 'lg')
GROUP BY table ORDER BY table;

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

SELECT '-- a setting is findable by an alias, carrying the same values';
SELECT name, value, source, alias_for FROM system.table_settings
WHERE database = currentDatabase() AND table = 'mt'
  AND name IN ('index_granularity', 'allow_experimental_block_number_column', 'enable_block_number_column')
ORDER BY name;

SELECT '-- the columns shared with system.merge_tree_settings agree, except where the definition differs';
-- A `MergeTree` table reports what `system.merge_tree_settings` reports, with the same column
-- names, types and meanings, for every setting the table's own definition does not state.
--
-- The definition-stated rows are excluded rather than enumerated. `loadFromQuery` writes the
-- immutable settings into the stored `CREATE` query, so which settings end up stated depends on
-- what a `<merge_tree>` configuration section sets - `index_granularity` always, and more on a
-- server that configures more. Naming the expected rows would pin this test to one server's
-- configuration; excluding them states the invariant that actually holds.
SELECT name FROM (
    SELECT name, value, `default`, changed, description, min, max, disallowed_values, readonly, type, is_obsolete, tier
    FROM system.table_settings
    WHERE database = currentDatabase() AND table = 'mt' AND alias_for = '' AND source != 'definition'
    EXCEPT
    SELECT name, value, `default`, changed, description, min, max, disallowed_values, readonly, type, is_obsolete, tier
    FROM system.merge_tree_settings)
ORDER BY name;

SELECT '-- settings the engine makes read-only are reported as such';
SELECT countIf(readonly) > 0 FROM system.table_settings WHERE database = currentDatabase() AND table = 'mt';

SELECT '-- filtering by database reaches the scan';
SELECT count() FROM system.table_settings WHERE database = 'database_that_does_not_exist';

DROP TABLE mt;
DROP TABLE jn;
DROP TABLE lg;
DROP TABLE plain;
DROP TABLE kfk;
