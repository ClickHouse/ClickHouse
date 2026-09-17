SET allow_experimental_time_series_table = 1;
SET enable_parallel_replicas = 0;

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
SELECT type, unit, help FROM timeSeriesMetricFamilies(ts) ORDER BY type;

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
CREATE TABLE ts ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0, insert_cache_max_size_bytes = 150
METRIC FAMILIES INNER ENGINE = MergeTree ORDER BY metric_family_name;

INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
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

SYSTEM STOP MERGES ext_metric_families;

INSERT INTO FUNCTION timeSeriesMetricFamilies(currentDatabase(), 'ts')
VALUES ('m', 'counter', 'bytes', 'function');
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM ext_metric_families WHERE metric_family_name = 'm';

INSERT INTO ext_metric_families
VALUES ('m', 'counter', 'bytes', 'inner');
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM ext_metric_families WHERE metric_family_name = 'm';

SYSTEM START MERGES ext_metric_families;
ALTER TABLE ext_metric_families DELETE WHERE metric_family_name = 'm' SETTINGS mutations_sync = 2;
SYSTEM STOP MERGES ext_metric_families;
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM ext_metric_families WHERE metric_family_name = 'm';

DROP TABLE ts;
DROP TABLE ext_metric_families;
