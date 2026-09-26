-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: this test compares local SQL and native plans.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS promql_native_rate_stale_markers;

CREATE TABLE promql_native_rate_stale_markers ENGINE = TimeSeries
SETTINGS version = 7, recent_samples_ttl_seconds = 0;

INSERT INTO promql_native_rate_stale_markers (metric_name, tags, samples)
SELECT 'reads', map('instance', 'stale'),
    [(toDateTime64(81, 3), 0.),
     (toDateTime64(91, 3), 10.),
     (toDateTime64(95, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
     (toDateTime64(100, 3), 20.)]
UNION ALL
SELECT 'reads', map('instance', 'ordinary'),
    [(toDateTime64(81, 3), 0.),
     (toDateTime64(91, 3), 10.),
     (toDateTime64(100, 3), reinterpretAsFloat64(toUInt64(0x7FF8000000000001)))]
UNION ALL
SELECT 'writes', map('instance', 'stale'),
    [(toDateTime64(81, 3), 0.),
     (toDateTime64(91, 3), 5.),
     (toDateTime64(100, 3), 10.)]
UNION ALL
SELECT 'writes', map('instance', 'ordinary'),
    [(toDateTime64(81, 3), 0.),
     (toDateTime64(91, 3), 5.),
     (toDateTime64(100, 3), 10.)];

-- A second insert creates another samples-table part, allowing the serial native path to merge
-- more than one ordered input stream when the underlying read schedules them separately.
INSERT INTO promql_native_rate_stale_markers (metric_name, tags, samples)
SELECT 'reads', map('instance', 'stale_second_part'),
    [(toDateTime64(81, 3), 0.),
     (toDateTime64(91, 3), 10.),
     (toDateTime64(95, 3), reinterpretAsFloat64(toUInt64(0x7FF0000000000002))),
     (toDateTime64(100, 3), 20.)]
UNION ALL
SELECT 'writes', map('instance', 'stale_second_part'),
    [(toDateTime64(81, 3), 0.),
     (toDateTime64(91, 3), 5.),
     (toDateTime64(100, 3), 10.)];

CREATE TEMPORARY TABLE stale_rate_sql AS
SELECT tags, samples
FROM prometheusQueryRange(promql_native_rate_stale_markers, 'rate(reads[20s])', 100, 100, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE stale_rate_sliced_native AS
SELECT tags, samples
FROM prometheusQueryRange(promql_native_rate_stale_markers, 'rate(reads[20s])', 100, 100, 10)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 0,
    enable_promql_native_parallel_processing = 0,
    max_threads = 4;

CREATE TEMPORARY TABLE stale_rate_raw_native AS
SELECT tags, samples
FROM prometheusQueryRange(promql_native_rate_stale_markers, 'rate(reads[20s])', 100, 100, 10)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 1,
    enable_promql_native_parallel_processing = 0,
    max_threads = 4;

-- A stale marker is omitted from the rate input; an ordinary NaN remains data.
SELECT
    count() = 3,
    countIf(abs(samples[1].2 - 1.) < 1e-10) = 2,
    countIf(isNaN(samples[1].2)) = 1
FROM stale_rate_sql;

SELECT
    count() = 3,
    countIf(abs(samples[1].2 - 1.) < 1e-10) = 2,
    countIf(isNaN(samples[1].2)) = 1
FROM stale_rate_sliced_native;

SELECT
    count() = 3,
    countIf(abs(samples[1].2 - 1.) < 1e-10) = 2,
    countIf(isNaN(samples[1].2)) = 1
FROM stale_rate_raw_native;

SELECT countIf(explain LIKE '%PromQLRangeRate%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(promql_native_rate_stale_markers, 'rate(reads[20s])', 100, 100, 10)
    SETTINGS enable_promql_native_plan = 1, enable_promql_native_raw_samples = 0
);

SELECT countIf(explain LIKE '%PromQLRangeRate%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(promql_native_rate_stale_markers, 'rate(reads[20s])', 100, 100, 10)
    SETTINGS enable_promql_native_plan = 1, enable_promql_native_raw_samples = 1
);

CREATE TEMPORARY TABLE stale_d06_sql AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_rate_stale_markers,
    'ceil(sum by(instance)(rate(reads[20s])+rate(writes[20s])))',
    100, 100, 10)
SETTINGS enable_promql_native_plan = 0;

CREATE TEMPORARY TABLE stale_d06_sliced_native AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_rate_stale_markers,
    'ceil(sum by(instance)(rate(reads[20s])+rate(writes[20s])))',
    100, 100, 10)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_raw_samples = 0;

CREATE TEMPORARY TABLE stale_d06_raw_native AS
SELECT tags, samples
FROM prometheusQueryRange(
    promql_native_rate_stale_markers,
    'ceil(sum by(instance)(rate(reads[20s])+rate(writes[20s])))',
    100, 100, 10)
SETTINGS enable_promql_native_plan = 1, enable_promql_native_raw_samples = 1;

SELECT
    count() = 3,
    countIf(samples[1].2 = 2.) = 2,
    countIf(isNaN(samples[1].2)) = 1
FROM stale_d06_sql;

SELECT
    count() = 3,
    countIf(samples[1].2 = 2.) = 2,
    countIf(isNaN(samples[1].2)) = 1
FROM stale_d06_sliced_native;

SELECT
    count() = 3,
    countIf(samples[1].2 = 2.) = 2,
    countIf(isNaN(samples[1].2)) = 1
FROM stale_d06_raw_native;

SELECT countIf(explain LIKE '%PromQLTwoRangeRates%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_rate_stale_markers,
        'ceil(sum by(instance)(rate(reads[20s])+rate(writes[20s])))',
        100, 100, 10)
    SETTINGS enable_promql_native_plan = 1, enable_promql_native_raw_samples = 0
);

DROP TABLE promql_native_rate_stale_markers SYNC;
