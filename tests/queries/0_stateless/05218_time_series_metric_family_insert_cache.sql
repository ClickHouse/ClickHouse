SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ext_metric_families;

CREATE TABLE ts ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0;

INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM timeSeriesMetricFamilies(ts);

INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'counter', 'bytes', 'second');
SELECT count() FROM timeSeriesMetricFamilies(ts);
SELECT type, unit, help FROM ts FINAL WHERE metric_family = 'm';

DROP TABLE ts;
CREATE TABLE ts ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0, insert_cache_max_size_bytes = 150;

INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m1', 'gauge', 'seconds', 'first');
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m2', 'gauge', 'seconds', 'second');
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m1', 'gauge', 'seconds', 'first');
SELECT count() FROM timeSeriesMetricFamilies(ts);

DROP TABLE ts;
CREATE TABLE ts ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0, insert_cache_max_size_bytes = 0;

INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM timeSeriesMetricFamilies(ts);

TRUNCATE TABLE ts;
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM timeSeriesMetricFamilies(ts);

DROP TABLE ts;
CREATE TABLE ext_metric_families
(
    metric_family_name String,
    type LowCardinality(String),
    unit LowCardinality(String),
    help String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name;

CREATE TABLE ts ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
METRIC FAMILIES ext_metric_families;

INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM ext_metric_families;

DROP TABLE ts;
DROP TABLE ext_metric_families;
