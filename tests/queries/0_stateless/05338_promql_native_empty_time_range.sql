-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Random settings limits: max_threads=(2, 2); serialize_query_plan=(0, 0)
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the test checks local native plan eligibility.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables need a local database.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET serialize_query_plan = 0;
SET max_threads = 2;
SET max_memory_usage = 2147483648;
SET min_promql_native_query_range_points = 0;
SET enable_promql_compensated_sum = 0;
SET enable_promql_native_parallel_processing = 0;
SET enable_promql_native_storage_fusion = 0;
SET enable_promql_native_raw_samples = 0;

DROP TABLE IF EXISTS promql_empty_dt;
DROP TABLE IF EXISTS promql_empty_u32;
DROP TABLE IF EXISTS promql_empty_dt64;

CREATE TABLE promql_empty_dt (samples Array(Tuple(DateTime('UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE promql_empty_u32 (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries;
CREATE TABLE promql_empty_dt64 (samples Array(Tuple(DateTime64(0, 'UTC'), Float64))) ENGINE = TimeSeries;

-- The data is nonempty. Only the requested timestamp interval is unrepresentable.
INSERT INTO promql_empty_dt (metric_name, tags, samples) VALUES
    ('reads', map('namespace', 'a', 'instance', 'one'),
        [(toDateTime(190, 'UTC'), 0.), (toDateTime(195, 'UTC'), 5.), (toDateTime(200, 'UTC'), 10.)]),
    ('writes', map('namespace', 'a', 'instance', 'one'),
        [(toDateTime(190, 'UTC'), 0.), (toDateTime(195, 'UTC'), 10.), (toDateTime(200, 'UTC'), 20.)]);
INSERT INTO promql_empty_u32 (metric_name, tags, samples) VALUES
    ('reads', map('namespace', 'a', 'instance', 'one'), [(190, 0.), (195, 5.), (200, 10.)]),
    ('writes', map('namespace', 'a', 'instance', 'one'), [(190, 0.), (195, 10.), (200, 20.)]);
INSERT INTO promql_empty_dt64 (metric_name, tags, samples) VALUES
    ('reads', map('namespace', 'a', 'instance', 'one'),
        [(toDateTime64(190, 0, 'UTC'), 0.), (toDateTime64(195, 0, 'UTC'), 5.), (toDateTime64(200, 0, 'UTC'), 10.)]),
    ('writes', map('namespace', 'a', 'instance', 'one'),
        [(toDateTime64(190, 0, 'UTC'), 0.), (toDateTime64(195, 0, 'UTC'), 10.), (toDateTime64(200, 0, 'UTC'), 20.)]);

-- Two evaluation points and a positive step keep native planning eligible.
-- For every selector, ceil(200.25 - 0.1 + 0.001) = 201 > floor(200.5) = 200.
-- An empty selector range has no identifier-set producer; this is not a native invariant failure.
-- The `ceil(sum by (...) (rate(...) + rate(...)))` shape admits a native two-rate fragment.
SET enable_promql_native_plan = 0;
CREATE TEMPORARY TABLE empty_sql AS
SELECT 'dt_rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'rate(reads[100ms])', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt_sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'sum by (namespace) (rate(reads[100ms]))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt_two_rates' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'ceil(sum by (namespace) (rate(reads[100ms]) + rate(writes[100ms])))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt_hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'clamp_max(sum by (namespace) (rate(reads[100ms])), 2)', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'rate(reads[100ms])', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'sum by (namespace) (rate(reads[100ms]))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_two_rates' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'ceil(sum by (namespace) (rate(reads[100ms]) + rate(writes[100ms])))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'clamp_max(sum by (namespace) (rate(reads[100ms])), 2)', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'rate(reads[100ms])', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'sum by (namespace) (rate(reads[100ms]))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_two_rates' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'ceil(sum by (namespace) (rate(reads[100ms]) + rate(writes[100ms])))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'clamp_max(sum by (namespace) (rate(reads[100ms])), 2)', 200.25, 200.5, 0.25);
SELECT 'sql_empty', count() FROM empty_sql;

SET enable_promql_native_plan = 1;
CREATE TEMPORARY TABLE empty_native_sliced AS
SELECT 'dt_rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'rate(reads[100ms])', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt_sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'sum by (namespace) (rate(reads[100ms]))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt_two_rates' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'ceil(sum by (namespace) (rate(reads[100ms]) + rate(writes[100ms])))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt_hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'clamp_max(sum by (namespace) (rate(reads[100ms])), 2)', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'rate(reads[100ms])', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'sum by (namespace) (rate(reads[100ms]))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_two_rates' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'ceil(sum by (namespace) (rate(reads[100ms]) + rate(writes[100ms])))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'clamp_max(sum by (namespace) (rate(reads[100ms])), 2)', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'rate(reads[100ms])', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'sum by (namespace) (rate(reads[100ms]))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_two_rates' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'ceil(sum by (namespace) (rate(reads[100ms]) + rate(writes[100ms])))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'clamp_max(sum by (namespace) (rate(reads[100ms])), 2)', 200.25, 200.5, 0.25);
SELECT 'native_sliced_empty', count() FROM empty_native_sliced;

SET enable_promql_native_raw_samples = 1;
CREATE TEMPORARY TABLE empty_native_raw AS
SELECT 'dt_rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'rate(reads[100ms])', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt_sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'sum by (namespace) (rate(reads[100ms]))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt_two_rates' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'ceil(sum by (namespace) (rate(reads[100ms]) + rate(writes[100ms])))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt_hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt, 'clamp_max(sum by (namespace) (rate(reads[100ms])), 2)', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'rate(reads[100ms])', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'sum by (namespace) (rate(reads[100ms]))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_two_rates' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'ceil(sum by (namespace) (rate(reads[100ms]) + rate(writes[100ms])))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'u32_hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_u32, 'clamp_max(sum by (namespace) (rate(reads[100ms])), 2)', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_rate' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'rate(reads[100ms])', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_sum' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'sum by (namespace) (rate(reads[100ms]))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_two_rates' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'ceil(sum by (namespace) (rate(reads[100ms]) + rate(writes[100ms])))', 200.25, 200.5, 0.25)
UNION ALL
SELECT 'dt64_hybrid' AS expression, tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'clamp_max(sum by (namespace) (rate(reads[100ms])), 2)', 200.25, 200.5, 0.25);
SELECT 'native_raw_empty', count() FROM empty_native_raw;

SELECT count() FROM (SELECT * FROM empty_sql EXCEPT ALL SELECT * FROM empty_native_sliced);
SELECT count() FROM (SELECT * FROM empty_native_sliced EXCEPT ALL SELECT * FROM empty_sql);
SELECT count() FROM (SELECT * FROM empty_sql EXCEPT ALL SELECT * FROM empty_native_raw);
SELECT count() FROM (SELECT * FROM empty_native_raw EXCEPT ALL SELECT * FROM empty_sql);

-- Negative fractional rounding is different from truncation towards zero.
-- The selector bounds round to [0, -1] for the signed second-precision table.
SELECT 'negative_dt64_empty', count() FROM prometheusQueryRange(
    promql_empty_dt64, 'rate(reads[100ms])',
    toDateTime64(-0.5, 3, 'UTC'), toDateTime64(-0.25, 3, 'UTC'), 0.25);
-- Both unsigned timestamp types have an empty clipped range beyond their domain.
SELECT 'unsigned_domain_empty', count() FROM
(
    SELECT * FROM prometheusQueryRange(promql_empty_dt, 'rate(reads[100ms])', 4294967297.25, 4294967297.5, 0.25)
    UNION ALL
    SELECT * FROM prometheusQueryRange(promql_empty_u32, 'rate(reads[100ms])', 4294967297.25, 4294967297.5, 0.25)
);

SET enable_promql_native_raw_samples = 0;
-- A representable range without matching IDs is not the typed-empty-range case.
SELECT 'no_matching_ids', count() FROM prometheusQueryRange(
    promql_empty_dt64, 'rate(reads{instance="absent"}[10s])', 195, 200, 5);

-- Positive controls prevent zero-result equality from hiding disabled native planning.
SELECT 'nonempty_dt_rate_plan', countIf(explain LIKE '%PromQLRangeRate%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(promql_empty_dt, 'rate(reads[10s])', 195, 200, 5)
);
SELECT 'nonempty_dt_sum_plan', countIf(explain LIKE '%PromQLRangeSumBy%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(
        promql_empty_dt, 'sum by (namespace) (rate(reads[10s]))', 195, 200, 5)
);
SELECT 'nonempty_dt_two_rates_plan', countIf(explain LIKE '%PromQLTwoRangeRates%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(
        promql_empty_dt, 'ceil(sum by (namespace) (rate(reads[10s]) + rate(writes[10s])))', 195, 200, 5)
);
SELECT 'nonempty_dt_hybrid_plan', countIf(explain LIKE '%PromQLRangeSumBy%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(
        promql_empty_dt, 'clamp_max(sum by (namespace) (rate(reads[10s])), 2)', 195, 200, 5)
);
SELECT 'nonempty_u32_rate_plan', countIf(explain LIKE '%PromQLRangeRate%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(promql_empty_u32, 'rate(reads[10s])', 195, 200, 5)
);
SELECT 'nonempty_u32_sum_plan', countIf(explain LIKE '%PromQLRangeSumBy%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(
        promql_empty_u32, 'sum by (namespace) (rate(reads[10s]))', 195, 200, 5)
);
SELECT 'nonempty_u32_two_rates_plan', countIf(explain LIKE '%PromQLTwoRangeRates%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(
        promql_empty_u32, 'ceil(sum by (namespace) (rate(reads[10s]) + rate(writes[10s])))', 195, 200, 5)
);
SELECT 'nonempty_u32_hybrid_plan', countIf(explain LIKE '%PromQLRangeSumBy%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(
        promql_empty_u32, 'clamp_max(sum by (namespace) (rate(reads[10s])), 2)', 195, 200, 5)
);
SELECT 'nonempty_rate_plan', countIf(explain LIKE '%PromQLRangeRate%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(promql_empty_dt64, 'rate(reads[10s])', 195, 200, 5)
);
SELECT 'nonempty_sum_plan', countIf(explain LIKE '%PromQLRangeSumBy%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(
        promql_empty_dt64, 'sum by (namespace) (rate(reads[10s]))', 195, 200, 5)
);
SELECT 'nonempty_two_rates_plan', countIf(explain LIKE '%PromQLTwoRangeRates%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(
        promql_empty_dt64, 'ceil(sum by (namespace) (rate(reads[10s]) + rate(writes[10s])))', 195, 200, 5)
);
SELECT 'nonempty_hybrid_plan', countIf(explain LIKE '%PromQLRangeSumBy%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(
        promql_empty_dt64, 'clamp_max(sum by (namespace) (rate(reads[10s])), 2)', 195, 200, 5)
);
SELECT 'nonempty_result', count(), sum(length(samples)) FROM prometheusQueryRange(
    promql_empty_dt64, 'rate(reads[10s])', 195, 200, 5);

-- Keep the raw positive control away from zero-based counter extrapolation.
INSERT INTO promql_empty_dt64 (metric_name, tags, samples) VALUES
    ('offset_reads', map('namespace', 'a', 'instance', 'one'),
        [(toDateTime64(190, 0, 'UTC'), 100.), (toDateTime64(195, 0, 'UTC'), 105.), (toDateTime64(200, 0, 'UTC'), 110.)]);
SET enable_promql_native_raw_samples = 1;
SET enable_promql_native_plan = 0;
CREATE TEMPORARY TABLE nonempty_raw_sql AS
SELECT tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'rate(offset_reads[10s])', 195, 200, 5);
SELECT 'sql_finite', count(), sum(length(samples)),
    sum(arrayCount(sample -> NOT isFinite(sample.2), samples)),
    sum(arrayCount(sample -> sample.2 != 1., samples))
FROM nonempty_raw_sql;

SET enable_promql_native_plan = 1;
SELECT 'raw_native_plan', countIf(explain LIKE '%PromQLRangeRate%') > 0 FROM
(
    EXPLAIN SELECT * FROM prometheusQueryRange(
        promql_empty_dt64, 'rate(offset_reads[10s])', 195, 200, 5)
);
CREATE TEMPORARY TABLE nonempty_raw_native AS
SELECT tags, samples FROM prometheusQueryRange(
    promql_empty_dt64, 'rate(offset_reads[10s])', 195, 200, 5);
SELECT 'raw_finite', count(), sum(length(samples)),
    sum(arrayCount(sample -> NOT isFinite(sample.2), samples)),
    sum(arrayCount(sample -> sample.2 != 1., samples))
FROM nonempty_raw_native;

-- Exact parity includes labels, timestamps, values and multiplicity.
SELECT 'sql_minus_raw', count() FROM
(
    SELECT tags, samples FROM nonempty_raw_sql
    EXCEPT ALL
    SELECT tags, samples FROM nonempty_raw_native
);
SELECT 'raw_minus_sql', count() FROM
(
    SELECT tags, samples FROM nonempty_raw_native
    EXCEPT ALL
    SELECT tags, samples FROM nonempty_raw_sql
);
DROP TABLE nonempty_raw_sql;
DROP TABLE nonempty_raw_native;

DROP TABLE promql_empty_dt;
DROP TABLE promql_empty_u32;
DROP TABLE promql_empty_dt64;
