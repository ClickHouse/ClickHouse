-- The "metrics" target table of a TimeSeries table names the column with the name of a metric family `metric_family`
-- since version 2, like the outer column of the TimeSeries table. Tables of older versions keep the old name
-- `metric_family_name` and stay readable and writable.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts_new;
DROP TABLE IF EXISTS ts_v1;
DROP TABLE IF EXISTS ts_copy;
DROP TABLE IF EXISTS ts_v1_custom;
DROP TABLE IF EXISTS ts_new_custom;
DROP TABLE IF EXISTS ts_ext;
DROP TABLE IF EXISTS ext_metrics_old;
DROP TABLE IF EXISTS ext_metrics_new;

SELECT '-- a new table names the column `metric_family`';
CREATE TABLE ts_new ENGINE = TimeSeries;
SELECT extract(create_table_query, 'version = (\d+)'),
       extract(create_table_query, 'METRICS INNER COLUMNS \((.*?)\) METRICS INNER ENGINE'),
       extract(create_table_query, 'METRICS INNER ENGINE = \w+ (ORDER BY .*?)(?: SETTINGS|$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_new';

INSERT INTO ts_new (metric_name, tags, time_series, metric_family, type, unit, help)
VALUES ('http_requests_total', {'job': 'test'}, [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests');
SELECT metric_family, type, unit, help FROM ts_new;
SELECT metric_name, metric_family, type FROM ts_new WHERE metric_name != '';
SELECT metric_family, type FROM timeSeriesMetrics(ts_new);

SELECT '-- a table of version 1 names the column `metric_family_name`';
CREATE TABLE ts_v1 ENGINE = TimeSeries SETTINGS version = 1;
SELECT extract(create_table_query, 'version = (\d+)'),
       extract(create_table_query, 'METRICS INNER COLUMNS \((.*?)\) METRICS INNER ENGINE'),
       extract(create_table_query, 'METRICS INNER ENGINE = \w+ (ORDER BY .*?)(?: SETTINGS|$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v1';

INSERT INTO ts_v1 (metric_name, tags, time_series, metric_family, type, unit, help)
VALUES ('http_requests_total', {'job': 'test'}, [(toDateTime64(1000, 3), 1.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests');
SELECT metric_family, type, unit, help FROM ts_v1;
SELECT metric_name, metric_family, type FROM ts_v1 WHERE metric_name != '';
SELECT metric_family_name, type FROM timeSeriesMetrics(ts_v1);

SELECT '-- an external metrics table must name the column according to the version';
CREATE TABLE ext_metrics_old (metric_family_name String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
CREATE TABLE ts_ext ENGINE = TimeSeries METRICS ext_metrics_old; -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE ts_ext ENGINE = TimeSeries SETTINGS version = 1 METRICS ext_metrics_old;
INSERT INTO ts_ext (metric_family, type, unit, help) VALUES ('http_requests', 'counter', 'requests', 'Total HTTP requests');
SELECT metric_family, type FROM ts_ext;
SELECT metric_family_name, type FROM ext_metrics_old;
DROP TABLE ts_ext;

CREATE TABLE ext_metrics_new (metric_family String, type String, unit String, help String) ENGINE = ReplacingMergeTree ORDER BY metric_family;
CREATE TABLE ts_ext ENGINE = TimeSeries SETTINGS version = 1 METRICS ext_metrics_new; -- { serverError THERE_IS_NO_COLUMN }
CREATE TABLE ts_ext ENGINE = TimeSeries METRICS ext_metrics_new;
INSERT INTO ts_ext (metric_family, type, unit, help) VALUES ('http_requests', 'counter', 'requests', 'Total HTTP requests');
SELECT metric_family, type FROM ts_ext;
SELECT metric_family, type FROM ext_metrics_new;
DROP TABLE ts_ext;

SELECT '-- CREATE AS a table of version 1: the generated column and sorting key get the new name';
CREATE TABLE ts_copy AS ts_v1;
SELECT extract(create_table_query, 'version = (\d+)'),
       extract(create_table_query, 'METRICS INNER COLUMNS \((.*?)\) METRICS INNER ENGINE'),
       extract(create_table_query, 'METRICS INNER ENGINE = \w+ (ORDER BY .*?)(?: SETTINGS|$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;

SELECT '-- CREATE AS a table of version 1 with a customized column and sorting key: they are copied with the new name';
CREATE TABLE ts_v1_custom ENGINE = TimeSeries SETTINGS version = 1
METRICS INNER COLUMNS (metric_family_name LowCardinality(String), extra UInt64 DEFAULT length(metric_family_name))
METRICS ENGINE = ReplacingMergeTree ORDER BY (metric_family_name, type);
SELECT extract(create_table_query, 'METRICS INNER COLUMNS \((.*?)\) METRICS INNER ENGINE'),
       extract(create_table_query, 'METRICS INNER ENGINE = \w+ (ORDER BY .*?)(?: SETTINGS|$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v1_custom';

CREATE TABLE ts_copy AS ts_v1_custom;
SELECT extract(create_table_query, 'version = (\d+)'),
       extract(create_table_query, 'METRICS INNER COLUMNS \((.*?)\) METRICS INNER ENGINE'),
       extract(create_table_query, 'METRICS INNER ENGINE = \w+ (ORDER BY .*?)(?: SETTINGS|$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
INSERT INTO ts_copy (metric_family, type, unit, help) VALUES ('http_requests', 'counter', 'requests', 'Total HTTP requests');
SELECT metric_family, type FROM ts_copy;
SELECT metric_family, extra FROM timeSeriesMetrics(ts_copy);
DROP TABLE ts_copy;

SELECT '-- CREATE AS a table of the latest version with an explicit version 1: the customized parts get the old name';
CREATE TABLE ts_new_custom ENGINE = TimeSeries
METRICS INNER COLUMNS (metric_family LowCardinality(String))
METRICS ENGINE = ReplacingMergeTree ORDER BY (metric_family, type);
CREATE TABLE ts_copy AS ts_new_custom ENGINE = TimeSeries SETTINGS version = 1;
SELECT extract(create_table_query, 'version = (\d+)'),
       extract(create_table_query, 'METRICS INNER COLUMNS \((.*?)\) METRICS INNER ENGINE'),
       extract(create_table_query, 'METRICS INNER ENGINE = \w+ (ORDER BY .*?)(?: SETTINGS|$)')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;

DROP TABLE ts_new_custom;
DROP TABLE ts_v1_custom;
DROP TABLE ext_metrics_new;
DROP TABLE ext_metrics_old;
DROP TABLE ts_v1;
DROP TABLE ts_new;
