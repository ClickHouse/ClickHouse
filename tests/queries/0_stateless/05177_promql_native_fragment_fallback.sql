-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test asserts which local query-plan step evaluates PromQL.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS promql_native_fallback_unordered;
DROP TABLE IF EXISTS promql_native_fallback_unordered_samples;

CREATE TABLE promql_native_fallback_unordered_samples
(
    id UUID,
    samples Array(Tuple(DateTime64(3), Float64)),
    bucket DateTime64(3),
    min_time DateTime64(3),
    max_time DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (bucket, id);

CREATE TABLE promql_native_fallback_unordered
ENGINE = TimeSeries
SETTINGS recent_samples_ttl_seconds = 0
SAMPLES promql_native_fallback_unordered_samples
TAGS INNER COLUMNS (id UUID);

INSERT INTO promql_native_fallback_unordered (metric_name, tags, time_series) VALUES
    ('m', map('dc', 'a', 'job', 'api'),
        [(toDateTime64(90, 3), 0.), (toDateTime64(100, 3), 10.), (toDateTime64(110, 3), 30.),
         (toDateTime64(120, 3), 55.), (toDateTime64(130, 3), 85.)]),
    ('m', map('dc', 'b', 'job', 'api'),
        [(toDateTime64(90, 3), 100.), (toDateTime64(100, 3), 120.), (toDateTime64(110, 3), 5.),
         (toDateTime64(120, 3), 25.), (toDateTime64(130, 3), 50.)]);

CREATE TEMPORARY TABLE unordered_h0_sql AS
SELECT tags, time_series
FROM prometheusQueryRange(
    promql_native_fallback_unordered,
    'clamp_max(sum by (dc) (rate(m{job="api"}[20])), 2)',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE unordered_h0_fallback AS
SELECT tags, time_series
FROM prometheusQueryRange(
    promql_native_fallback_unordered,
    'clamp_max(sum by (dc) (rate(m{job="api"}[20])), 2)',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

SELECT count() FROM
(
    SELECT tags, time_series FROM unordered_h0_fallback
    EXCEPT ALL
    SELECT tags, time_series FROM unordered_h0_sql
);

SELECT count() FROM
(
    SELECT tags, time_series FROM unordered_h0_sql
    EXCEPT ALL
    SELECT tags, time_series FROM unordered_h0_fallback
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_fallback_unordered,
        'clamp_max(sum by (dc) (rate(m{job="api"}[20])), 2)',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

CREATE TEMPORARY TABLE unordered_h1_sql AS
SELECT tags, time_series
FROM prometheusQueryRange(
    promql_native_fallback_unordered,
    'topk(1, sum by (dc) (rate(m{job="api"}[20])))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE unordered_h1_fallback AS
SELECT tags, time_series
FROM prometheusQueryRange(
    promql_native_fallback_unordered,
    'topk(1, sum by (dc) (rate(m{job="api"}[20])))',
    110, 130, 10)
SETTINGS enable_promql_native_plan = 1;

SELECT count() FROM
(
    SELECT tags, time_series FROM unordered_h1_fallback
    EXCEPT ALL
    SELECT tags, time_series FROM unordered_h1_sql
);

SELECT count() FROM
(
    SELECT tags, time_series FROM unordered_h1_sql
    EXCEPT ALL
    SELECT tags, time_series FROM unordered_h1_fallback
);

SELECT countIf(explain LIKE '%PromQLRangeSumBy%')
FROM
(
    EXPLAIN
    SELECT *
    FROM prometheusQueryRange(
        promql_native_fallback_unordered,
        'topk(1, sum by (dc) (rate(m{job="api"}[20])))',
        110, 130, 10)
    SETTINGS enable_promql_native_plan = 1
);

DROP TABLE promql_native_fallback_unordered SYNC;
DROP TABLE promql_native_fallback_unordered_samples SYNC;
