-- Tables of versions before 6 name the column of the "metric families" target table with the name of a metric family
-- `metric_family_name` (see TimeSeriesVersion.h) and stay readable and writable. The generation of the column and
-- the checks of its name are covered by the unit test gtest_normalize_time_series_definition.cpp.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_v5;
CREATE TABLE ts_v5 ENGINE = TimeSeries SETTINGS version = 5;

INSERT INTO ts_v5 (metric_name, tags, samples, metric_family, type, unit, help)
VALUES ('http_requests_total', {'job': 'test'}, [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests');

SELECT metric_family, type, unit, help FROM ts_v5;
SELECT metric_name, metric_family, type FROM ts_v5 WHERE metric_name != '';
SELECT * FROM timeSeriesMetricFamilies(ts_v5) FORMAT TSVWithNamesAndTypes;

DROP TABLE ts_v5;
