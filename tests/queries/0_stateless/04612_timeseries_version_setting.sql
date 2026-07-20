-- Tags: no-fasttest, no-replicated-database
-- - PromQL requires ANTLR4 support which is disabled in the fast-test build.
-- - The experimental TimeSeries table engine does not round-trip through DatabaseReplicated.
--
-- Coverage for the `version` setting of the TimeSeries table engine (see TimeSeriesVersion.h):
-- it is stamped automatically at CREATE time, persists in the table metadata, and cannot be changed.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_version;
DROP TABLE IF EXISTS ts_version_copy;
DROP TABLE IF EXISTS ts_version_explicit;
DROP TABLE IF EXISTS ts_version_bad;

SELECT '--- the version is stamped into a new table ---';
CREATE TABLE ts_version ENGINE = TimeSeries SETTINGS tags_to_columns = {'job':'job'};
SELECT extract(create_table_query, 'version = (\d+)'), extract(engine_full, 'version = (\d+)')
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_version';

SELECT '--- the version survives DETACH/ATTACH ---';
DETACH TABLE ts_version;
ATTACH TABLE ts_version;
SELECT extract(create_table_query, 'version = (\d+)')
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_version';

SELECT '--- PromQL works on a table with the latest version ---';
SELECT count() FROM prometheusQuery(ts_version, 'up', 1000);

SELECT '--- the version cannot be altered ---';
ALTER TABLE ts_version MODIFY SETTING version = 2; -- { serverError NOT_IMPLEMENTED }
ALTER TABLE ts_version RESET SETTING version; -- { serverError NOT_IMPLEMENTED }

SELECT '--- altering another setting does not drop the version or other settings from the metadata ---';
ALTER TABLE ts_version MODIFY SETTING filter_by_min_time_and_max_time = false;
SELECT extract(create_table_query, 'version = (\d+)'),
       position(create_table_query, 'tags_to_columns') > 0,
       position(create_table_query, 'filter_by_min_time_and_max_time') > 0
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_version';

SELECT '--- CREATE AS copies the version ---';
CREATE TABLE ts_version_copy AS ts_version;
SELECT extract(create_table_query, 'version = (\d+)')
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_version_copy';

SELECT '--- an explicit version in CREATE: the latest one is allowed, others are rejected ---';
CREATE TABLE ts_version_explicit ENGINE = TimeSeries SETTINGS version = 1;
SELECT extract(create_table_query, 'version = (\d+)')
    FROM system.tables WHERE database = currentDatabase() AND name = 'ts_version_explicit';
CREATE TABLE ts_version_bad ENGINE = TimeSeries SETTINGS version = 0; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_version_bad ENGINE = TimeSeries SETTINGS version = 999; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE ts_version_explicit;
DROP TABLE ts_version_copy;
DROP TABLE ts_version;
