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
SET enable_promql_native_plan = 0;

DROP TABLE IF EXISTS promql_raw_bounds_u32;
DROP TABLE IF EXISTS promql_raw_bounds_dt;
DROP TABLE IF EXISTS promql_raw_bounds_dt64;

CREATE TABLE promql_raw_bounds_u32 (samples Array(Tuple(UInt32, Float64))) ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;
CREATE TABLE promql_raw_bounds_dt (samples Array(Tuple(DateTime('UTC'), Float64))) ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;
CREATE TABLE promql_raw_bounds_dt64 (samples Array(Tuple(DateTime64(0, 'UTC'), Float64))) ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, recent_samples_ttl_seconds = 0;

-- Offset counters isolate timestamp normalization from zero-based counter extrapolation.
-- Evaluating at [2, 3] with a three-second window gives a selector range of [-0.999, 3].
-- The unsigned timestamp domains clip its lower bound to zero.
INSERT INTO promql_raw_bounds_u32 (metric_name, tags, samples) VALUES
    ('reads', map('namespace', 'a', 'instance', 'one'),
        [(0, 100.), (1, 104.), (2, 108.), (3, 112.),
         (4294967292, 100.), (4294967293, 104.), (4294967294, 108.), (4294967295, 112.)]),
    ('writes', map('namespace', 'a', 'instance', 'one'),
        [(0, 200.), (1, 208.), (2, 216.), (3, 224.),
         (4294967292, 200.), (4294967293, 208.), (4294967294, 216.), (4294967295, 224.)]);
INSERT INTO promql_raw_bounds_dt (metric_name, tags, samples) VALUES
    ('reads', map('namespace', 'a', 'instance', 'one'),
        [(toDateTime(0, 'UTC'), 100.), (toDateTime(1, 'UTC'), 104.),
         (toDateTime(2, 'UTC'), 108.), (toDateTime(3, 'UTC'), 112.)]),
    ('writes', map('namespace', 'a', 'instance', 'one'),
        [(toDateTime(0, 'UTC'), 200.), (toDateTime(1, 'UTC'), 208.),
         (toDateTime(2, 'UTC'), 216.), (toDateTime(3, 'UTC'), 224.)]);
INSERT INTO promql_raw_bounds_dt64 (metric_name, tags, samples) VALUES
    ('reads', map('namespace', 'a', 'instance', 'one'),
        [(toDateTime64(-6, 0, 'UTC'), 100.), (toDateTime64(-5, 0, 'UTC'), 104.),
         (toDateTime64(-4, 0, 'UTC'), 108.), (toDateTime64(-3, 0, 'UTC'), 112.),
         (toDateTime64(-2, 0, 'UTC'), 116.), (toDateTime64(-1, 0, 'UTC'), 120.),
         (toDateTime64(0, 0, 'UTC'), 124.), (toDateTime64(1, 0, 'UTC'), 128.),
         (toDateTime64(2, 0, 'UTC'), 132.), (toDateTime64(3, 0, 'UTC'), 136.)]),
    ('writes', map('namespace', 'a', 'instance', 'one'),
        [(toDateTime64(-6, 0, 'UTC'), 200.), (toDateTime64(-5, 0, 'UTC'), 208.),
         (toDateTime64(-4, 0, 'UTC'), 216.), (toDateTime64(-3, 0, 'UTC'), 224.),
         (toDateTime64(-2, 0, 'UTC'), 232.), (toDateTime64(-1, 0, 'UTC'), 240.),
         (toDateTime64(0, 0, 'UTC'), 248.), (toDateTime64(1, 0, 'UTC'), 256.),
         (toDateTime64(2, 0, 'UTC'), 264.), (toDateTime64(3, 0, 'UTC'), 272.)]);

-- The fractional signed ranges normalize to [0, 3] and [-5, -2], respectively.
-- The upper-domain case normalizes to [4294967293, 4294967295] and stays nonempty.
-- Each case covers a complete native `rate` root and a native two-rate fragment in a hybrid query.
CREATE TEMPORARY TABLE raw_bounds_sql AS
SELECT 'u32_integer_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'rate(reads[3s])', 2, 3, 1)
UNION ALL
SELECT 'u32_integer_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2, 3, 1)
UNION ALL
SELECT 'dt_integer_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt, 'rate(reads[3s])', 2, 3, 1)
UNION ALL
SELECT 'dt_integer_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2, 3, 1)
UNION ALL
SELECT 'dt64_fraction_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'rate(reads[3s])', 2.25, 3.25, 1)
UNION ALL
SELECT 'dt64_fraction_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2.25, 3.25, 1)
UNION ALL
SELECT 'dt64_negative_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'rate(reads[3s])', toDateTime64(-2.75, 3, 'UTC'), toDateTime64(-1.75, 3, 'UTC'), 1)
UNION ALL
SELECT 'dt64_negative_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))',
    toDateTime64(-2.75, 3, 'UTC'), toDateTime64(-1.75, 3, 'UTC'), 1)
UNION ALL
SELECT 'u32_upper_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'rate(reads[3s])', 4294967295.25, 4294967296.25, 1)
UNION ALL
SELECT 'u32_upper_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 4294967295.25, 4294967296.25, 1);

-- Exact labels, evaluation timestamps and values make equality non-vacuous.
SELECT case_name, tags, arrayMap(sample -> tuple(toUnixTimestamp64Milli(sample.1), sample.2), samples)
FROM raw_bounds_sql ORDER BY case_name;
SELECT 'sql_points', count(), sum(length(samples)),
    sum(arrayCount(sample -> NOT isFinite(sample.2), samples)) FROM raw_bounds_sql;

SET enable_promql_native_plan = 1;
CREATE TEMPORARY TABLE raw_bounds_sliced AS
SELECT 'u32_integer_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'rate(reads[3s])', 2, 3, 1)
UNION ALL
SELECT 'u32_integer_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2, 3, 1)
UNION ALL
SELECT 'dt_integer_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt, 'rate(reads[3s])', 2, 3, 1)
UNION ALL
SELECT 'dt_integer_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2, 3, 1)
UNION ALL
SELECT 'dt64_fraction_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'rate(reads[3s])', 2.25, 3.25, 1)
UNION ALL
SELECT 'dt64_fraction_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2.25, 3.25, 1)
UNION ALL
SELECT 'dt64_negative_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'rate(reads[3s])', toDateTime64(-2.75, 3, 'UTC'), toDateTime64(-1.75, 3, 'UTC'), 1)
UNION ALL
SELECT 'dt64_negative_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))',
    toDateTime64(-2.75, 3, 'UTC'), toDateTime64(-1.75, 3, 'UTC'), 1)
UNION ALL
SELECT 'u32_upper_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'rate(reads[3s])', 4294967295.25, 4294967296.25, 1)
UNION ALL
SELECT 'u32_upper_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 4294967295.25, 4294967296.25, 1);
SELECT 'sliced_points', count(), sum(length(samples)),
    sum(arrayCount(sample -> NOT isFinite(sample.2), samples)) FROM raw_bounds_sliced;

SET enable_promql_native_raw_samples = 1;
CREATE TEMPORARY TABLE raw_bounds_raw AS
SELECT 'u32_integer_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'rate(reads[3s])', 2, 3, 1)
UNION ALL
SELECT 'u32_integer_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2, 3, 1)
UNION ALL
SELECT 'dt_integer_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt, 'rate(reads[3s])', 2, 3, 1)
UNION ALL
SELECT 'dt_integer_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2, 3, 1)
UNION ALL
SELECT 'dt64_fraction_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'rate(reads[3s])', 2.25, 3.25, 1)
UNION ALL
SELECT 'dt64_fraction_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2.25, 3.25, 1)
UNION ALL
SELECT 'dt64_negative_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'rate(reads[3s])', toDateTime64(-2.75, 3, 'UTC'), toDateTime64(-1.75, 3, 'UTC'), 1)
UNION ALL
SELECT 'dt64_negative_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_dt64, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))',
    toDateTime64(-2.75, 3, 'UTC'), toDateTime64(-1.75, 3, 'UTC'), 1)
UNION ALL
SELECT 'u32_upper_rate' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'rate(reads[3s])', 4294967295.25, 4294967296.25, 1)
UNION ALL
SELECT 'u32_upper_two_rates' AS case_name, tags, samples FROM prometheusQueryRange(
    promql_raw_bounds_u32, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 4294967295.25, 4294967296.25, 1);
SELECT 'raw_points', count(), sum(length(samples)),
    sum(arrayCount(sample -> NOT isFinite(sample.2), samples)) FROM raw_bounds_raw;

-- Preserve labels, timestamps, values and multiplicity; row order may differ.
SELECT 'sql_minus_sliced', count() FROM
    (SELECT * FROM raw_bounds_sql EXCEPT ALL SELECT * FROM raw_bounds_sliced);
SELECT 'sliced_minus_sql', count() FROM
    (SELECT * FROM raw_bounds_sliced EXCEPT ALL SELECT * FROM raw_bounds_sql);
SELECT 'sql_minus_raw', count() FROM
    (SELECT * FROM raw_bounds_sql EXCEPT ALL SELECT * FROM raw_bounds_raw);
SELECT 'raw_minus_sql', count() FROM
    (SELECT * FROM raw_bounds_raw EXCEPT ALL SELECT * FROM raw_bounds_sql);

-- Match the native step's immediate input header, not a physical reader or final result column.
-- The two-rate check proves a raw native fragment inside the hybrid query.
SELECT 'u32_raw_root_input', match(arrayStringConcat(groupArray(replaceRegexpOne(explain, '^ +', '')), '\n'),
    'PromQLRangeRate\nInput headers: #0\nid [^\n]+\nbucket [^\n]+\nsamples Array\\(Tuple\\(UInt32, Float64\\)\\)')
FROM
(
    EXPLAIN PLAN header = 0, input_headers = 1, description = 0, pretty = 0
    SELECT * FROM prometheusQueryRange(promql_raw_bounds_u32, 'rate(reads[3s])', 2, 3, 1)
);
SELECT 'u32_raw_two_rates_input', match(arrayStringConcat(groupArray(replaceRegexpOne(explain, '^ +', '')), '\n'),
    'PromQLTwoRangeRates\nInput headers: #0\nid [^\n]+\nbucket [^\n]+\nsamples Array\\(Tuple\\(UInt32, Float64\\)\\)')
FROM
(
    EXPLAIN PLAN header = 0, input_headers = 1, description = 0, pretty = 0
    SELECT * FROM prometheusQueryRange(
        promql_raw_bounds_u32, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2, 3, 1)
);
SELECT 'dt_raw_root_input', match(arrayStringConcat(groupArray(replaceRegexpOne(explain, '^ +', '')), '\n'),
    'PromQLRangeRate\nInput headers: #0\nid [^\n]+\nbucket [^\n]+\nsamples Array\\(Tuple\\(DateTime\\(\'UTC\'\\), Float64\\)\\)')
FROM
(
    EXPLAIN PLAN header = 0, input_headers = 1, description = 0, pretty = 0
    SELECT * FROM prometheusQueryRange(promql_raw_bounds_dt, 'rate(reads[3s])', 2, 3, 1)
);
SELECT 'dt_raw_two_rates_input', match(arrayStringConcat(groupArray(replaceRegexpOne(explain, '^ +', '')), '\n'),
    'PromQLTwoRangeRates\nInput headers: #0\nid [^\n]+\nbucket [^\n]+\nsamples Array\\(Tuple\\(DateTime\\(\'UTC\'\\), Float64\\)\\)')
FROM
(
    EXPLAIN PLAN header = 0, input_headers = 1, description = 0, pretty = 0
    SELECT * FROM prometheusQueryRange(
        promql_raw_bounds_dt, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2, 3, 1)
);
SELECT 'dt64_raw_root_input', match(arrayStringConcat(groupArray(replaceRegexpOne(explain, '^ +', '')), '\n'),
    'PromQLRangeRate\nInput headers: #0\nid [^\n]+\nbucket [^\n]+\nsamples Array\\(Tuple\\(DateTime64\\(0, \'UTC\'\\), Float64\\)\\)')
FROM
(
    EXPLAIN PLAN header = 0, input_headers = 1, description = 0, pretty = 0
    SELECT * FROM prometheusQueryRange(promql_raw_bounds_dt64, 'rate(reads[3s])', 2.25, 3.25, 1)
);
SELECT 'dt64_raw_two_rates_input', match(arrayStringConcat(groupArray(replaceRegexpOne(explain, '^ +', '')), '\n'),
    'PromQLTwoRangeRates\nInput headers: #0\nid [^\n]+\nbucket [^\n]+\nsamples Array\\(Tuple\\(DateTime64\\(0, \'UTC\'\\), Float64\\)\\)')
FROM
(
    EXPLAIN PLAN header = 0, input_headers = 1, description = 0, pretty = 0
    SELECT * FROM prometheusQueryRange(
        promql_raw_bounds_dt64, 'ceil(sum by(namespace)(rate(reads[3s])+rate(writes[3s])))', 2.25, 3.25, 1)
);

DROP TABLE raw_bounds_sql;
DROP TABLE raw_bounds_sliced;
DROP TABLE raw_bounds_raw;
DROP TABLE promql_raw_bounds_u32;
DROP TABLE promql_raw_bounds_dt;
DROP TABLE promql_raw_bounds_dt64;
