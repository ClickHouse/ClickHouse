SET allow_experimental_time_series_table = 1;
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts2;
DROP TABLE IF EXISTS ext_metric_families;
DROP DATABASE IF EXISTS {CLICKHOUSE_DATABASE_1:Identifier};

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

CREATE DATABASE {CLICKHOUSE_DATABASE_1:Identifier} ENGINE = Memory;
CREATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.ts ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
METRIC FAMILIES INNER ENGINE = ReplacingMergeTree
ORDER BY metric_family_name;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
TRUNCATE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.metricfamilies.ts`;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.metricfamilies.ts`;

INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts (metric_family, type, unit, help)
VALUES ('m2', 'gauge', 'seconds', 'second');
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.metricfamilies.ts`;
OPTIMIZE TABLE {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.metricfamilies.ts` FINAL;
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.metricfamilies.ts`;
INSERT INTO {CLICKHOUSE_DATABASE_1:Identifier}.ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM {CLICKHOUSE_DATABASE_1:Identifier}.`.inner.metricfamilies.ts`;

ALTER TABLE ts MODIFY SETTING insert_cache_max_size_bytes = 0;
INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM timeSeriesMetricFamilies(ts);

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
METRIC FAMILIES INNER COLUMNS (extra String DEFAULT '')
METRIC FAMILIES INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name;

INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
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

CREATE TABLE ts2 ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
METRIC FAMILIES ext_metric_families;

INSERT INTO ts (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
INSERT INTO ts2 (metric_family, type, unit, help)
VALUES ('m', 'gauge', 'seconds', 'first');
SELECT count() FROM ext_metric_families;

DROP TABLE ts;
DROP TABLE ts2;
DROP TABLE ext_metric_families;
DROP DATABASE {CLICKHOUSE_DATABASE_1:Identifier};
