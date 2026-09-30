-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test asserts which local query-plan step evaluates PromQL.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS promql_old_timeseries_version;

CREATE TABLE promql_old_timeseries_version ENGINE = TimeSeries
SETTINGS version = 5, recent_samples_ttl_seconds = 0;

INSERT INTO promql_old_timeseries_version (metric_name, tags, samples) VALUES
    ('m', map('dc', 'a', 'host', 'h1'),
        [(toDateTime64(90, 3), 0.), (toDateTime64(100, 3), 10.),
         (toDateTime64(110, 3), 30.), (toDateTime64(120, 3), 55.),
         (toDateTime64(130, 3), 85.)]),
    ('m', map('dc', 'a', 'host', 'h2'),
        [(toDateTime64(90, 3), 100.), (toDateTime64(100, 3), 120.),
         (toDateTime64(110, 3), 5.), (toDateTime64(120, 3), 25.),
         (toDateTime64(130, 3), 50.)]);

CREATE TEMPORARY TABLE promql_old_sql AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_old_timeseries_version,
    'sum by (dc) (rate(m[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE promql_old_hybrid AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_old_timeseries_version,
    'sum by (dc) (rate(m[20]))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

-- A pre-bucket-layout table stays on the existing SQL path and produces exactly
-- the same result when native planning is enabled.
SELECT count()
FROM
(
    SELECT tags, samples FROM promql_old_hybrid
    EXCEPT ALL
    SELECT tags, samples FROM promql_old_sql
);

SELECT count()
FROM
(
    SELECT tags, samples FROM promql_old_sql
    EXCEPT ALL
    SELECT tags, samples FROM promql_old_hybrid
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_old_timeseries_version,
        'sum by (dc) (rate(m[20]))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

DROP TABLE promql_old_timeseries_version;
