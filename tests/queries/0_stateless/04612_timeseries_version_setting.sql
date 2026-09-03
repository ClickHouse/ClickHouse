-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: plain `DETACH TABLE` is not allowed there, only `DETACH TABLE PERMANENTLY`.
--
-- Coverage for the `version` setting of the TimeSeries table engine (see TimeSeriesVersion.h):
-- it is pinned automatically at CREATE time, persists in the table metadata, and cannot be changed.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_version;
DROP TABLE IF EXISTS ts_version_1;
DROP TABLE IF EXISTS ts_version_0;
DROP TABLE IF EXISTS ts_version_1_copy;
DROP TABLE IF EXISTS ts_version_0_copy;
DROP TABLE IF EXISTS ts_version_bad;

SELECT '--- the version is pinned into a new table ---';
CREATE TABLE ts_version ENGINE = TimeSeries SETTINGS tags_to_columns = {'job':'job'};
SELECT extract(create_table_query, 'version = (\d+)'), extract(engine_full, 'version = (\d+)')
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_version';

SELECT '--- the version survives DETACH/ATTACH ---';
DETACH TABLE ts_version;
ATTACH TABLE ts_version;
SELECT extract(create_table_query, 'version = (\d+)')
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_version';

SELECT '--- the version cannot be altered ---';
ALTER TABLE ts_version MODIFY SETTING version = 2; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE ts_version RESET SETTING version; -- { serverError NOT_IMPLEMENTED }

SELECT '--- altering another setting does not drop the version or other settings from the metadata ---';
ALTER TABLE ts_version MODIFY SETTING filter_by_min_time_and_max_time = false;
SELECT extract(create_table_query, 'version = (\d+)'),
       position(create_table_query, 'tags_to_columns') > 0,
       position(create_table_query, 'filter_by_min_time_and_max_time') > 0,
       position(create_table_query, 'recent_samples_ttl_seconds') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_version';

SELECT '--- an explicit version in CREATE: supported versions are allowed, unknown ones are rejected ---';
CREATE TABLE ts_version_1 ENGINE = TimeSeries SETTINGS version = 1;
SELECT extract(create_table_query, 'version = (\d+)')
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_version_1';
CREATE TABLE ts_version_0 ENGINE = TimeSeries SETTINGS version = 0;
SELECT extract(create_table_query, 'version = (\d+)')
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_version_0';
CREATE TABLE ts_version_bad ENGINE = TimeSeries SETTINGS version = 999; -- { serverError INVALID_SETTING_VALUE }

SELECT '--- PromQL works on tables of every supported version ---';
SELECT count() FROM prometheusQuery(ts_version_0, 'up', 1000);
SELECT count() FROM prometheusQuery(ts_version_1, 'up', 1000);

SELECT '--- CREATE AS copies the version ---';
CREATE TABLE ts_version_0_copy AS ts_version_0;
CREATE TABLE ts_version_1_copy AS ts_version_1;
SELECT name, extract(create_table_query, 'version = (\d+)')
    FROM system.tables WHERE database = currentDatabase() AND name IN ('ts_version_0_copy', 'ts_version_1_copy') ORDER BY name;

DROP TABLE ts_version_0_copy;
DROP TABLE ts_version_1_copy;
DROP TABLE ts_version_0;
DROP TABLE ts_version_1;
DROP TABLE ts_version;
