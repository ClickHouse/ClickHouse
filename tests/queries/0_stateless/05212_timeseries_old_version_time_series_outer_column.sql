-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The outer column `time_series` of a TimeSeries table was renamed to `samples` in version 3 (see TimeSeriesVersion.h),
-- and a table of an earlier version keeps the old name. The generation of the column is covered by
-- gtest_normalize_time_series_definition.cpp, this test checks that such a table can be written and read,
-- and that `prometheusQueryRange` returns the column with samples under the name the table uses.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_version_2;

CREATE TABLE ts_version_2 ENGINE = TimeSeries SETTINGS version = 2;
INSERT INTO ts_version_2 (metric_name, tags, time_series) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 3), 1), (toDateTime64(1060, 3), 2)]);
SELECT metric_name, time_series FROM ts_version_2;
SELECT * FROM prometheusQueryRange(ts_version_2, 'up', 1000, 1060, 60) FORMAT TSVWithNamesAndTypes;

DROP TABLE ts_version_2;
