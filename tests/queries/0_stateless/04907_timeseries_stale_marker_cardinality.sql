-- Tags: no-fasttest
-- Tag no-fasttest: the TimeSeries table engine is not available in the fast-test build.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_flags;
CREATE TABLE ts_flags ENGINE = TimeSeries;

-- A row with marker flags but no samples must be rejected, not silently dropped:
-- the flags are parallel to the samples, one per sample or none.
INSERT INTO ts_flags (metric_name, tags, time_series, is_stale_marker) VALUES ('up', map('instance', 'h1'), [], [1]); -- { serverError INCORRECT_DATA }
INSERT INTO ts_flags (metric_name, tags, time_series, is_stale_marker) VALUES ('up', map('instance', 'h1'), [(toDateTime64(100, 3), 1), (toDateTime64(101, 3), 2)], [0]); -- { serverError INCORRECT_DATA }

-- On a table whose samples table has the column, a flag on any sample works; see
-- 04903 and the integration tests for the legacy-table degradation rules.
-- One flag per sample, and no flags at all, both work.
INSERT INTO ts_flags (metric_name, tags, time_series, is_stale_marker) VALUES ('up', map('instance', 'h1'), [(toDateTime64(100, 3), 1)], [0]);
INSERT INTO ts_flags (metric_name, tags, time_series) VALUES ('up', map('instance', 'h2'), [(toDateTime64(100, 3), 2)]);
SELECT count() FROM timeSeriesData(ts_flags);

DROP TABLE ts_flags;
