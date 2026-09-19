-- Tags: no-replicated-database
-- Tag no-replicated-database: plain `DETACH TABLE` is not allowed there, only `DETACH TABLE PERMANENTLY`.
--
-- `TimeSeries` records what its definition states in the settings object, so `system.table_settings` reads the
-- source from there rather than from the stored `CREATE` query. The settings are loaded from a normalised copy of
-- the definition, and the names the definition states are recorded afterwards; the two have to agree on `CREATE`,
-- after `ALTER ... MODIFY SETTING` and `RESET SETTING`, which write the clause back in a canonical form, and after
-- the table is loaded again from what it stored. Values are left out: some depend on the build.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_definition;

CREATE TABLE ts_definition ENGINE = TimeSeries
SETTINGS store_min_time_and_max_time = 0, filter_by_min_time_and_max_time = 0, samples_index_granularity = 1024;

SELECT '-- CREATE';
SELECT name, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'ts_definition' AND source != 'default'
ORDER BY name;

SELECT '-- MODIFY SETTING';
ALTER TABLE ts_definition MODIFY SETTING id_generator = 'tuple(sipHash64(metric_name), toLowCardinality(reinterpretAsUUID(sipHash128(tags))))';
SELECT name, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'ts_definition' AND source != 'default'
ORDER BY name;

SELECT '-- RESET SETTING';
ALTER TABLE ts_definition RESET SETTING id_generator;
SELECT name, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'ts_definition' AND source != 'default'
ORDER BY name;

SELECT '-- loaded again from what it stored';
DETACH TABLE ts_definition;
ATTACH TABLE ts_definition;
SELECT name, source FROM system.table_settings
WHERE database = currentDatabase() AND table = 'ts_definition' AND source != 'default'
ORDER BY name;

DROP TABLE ts_definition;
