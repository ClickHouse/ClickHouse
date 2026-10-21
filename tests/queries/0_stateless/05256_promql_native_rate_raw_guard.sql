-- Tags: no-fasttest, no-parallel-replicas, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: the query checks the local native plan.
-- Tag no-replicated-database: deferred drops of `TimeSeries` inner tables are incompatible with replicated databases.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET serialize_query_plan = 0;

DROP TABLE IF EXISTS promql_native_rate_raw_guard;

CREATE TABLE promql_native_rate_raw_guard ENGINE = TimeSeries
SETTINGS samples_bucket_step_seconds = 60, samples_index_granularity = 1, recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree
SETTINGS min_bytes_for_wide_part = 0;

-- Both series occupy the same bucket and overlap the requested time range.
-- The unselected physical row exceeds the raw limit even though the selected row does not.
INSERT INTO promql_native_rate_raw_guard (metric_name, tags, samples) VALUES
    ('reads', map('instance', 'selected'),
        [(toDateTime64(0, 3), 0.),
         (toDateTime64(10, 3), 10.),
         (toDateTime64(20, 3), 20.),
         (toDateTime64(30, 3), 30.)]),
    ('reads', map('instance', 'oversized'),
        [(toDateTime64(0, 3), 0.),
         (toDateTime64(10, 3), 10.),
         (toDateTime64(20, 3), 20.),
         (toDateTime64(30, 3), 30.),
         (toDateTime64(40, 3), 40.)]);

-- Check the physical rows, not merely the input arrays.
SELECT count() = 2 AND uniqExact(bucket) = 1 AND countIf(length(samples) = 4) = 1 AND countIf(length(samples) = 5) = 1
FROM timeSeriesSamples(promql_native_rate_raw_guard);

SELECT count() = 1 AND min(length(samples)) = 4
FROM timeSeriesSamples(promql_native_rate_raw_guard)
WHERE id IN
(
    SELECT id FROM timeSeriesTags(promql_native_rate_raw_guard)
    WHERE metric_name = 'reads' AND tags['instance'] = 'selected'
);

-- An SQL fallback would pass without exercising the raw guard.
SELECT countIf(explain LIKE '%(PromQLRangeRate)%') > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT *
    FROM prometheusQueryRange(
        promql_native_rate_raw_guard,
        'rate(reads{instance="selected"}[20s])',
        20, 30, 10)
    SETTINGS
        enable_promql_native_plan = 1,
        enable_promql_native_raw_samples = 1,
        max_promql_native_rate_samples_per_series = 4,
        use_index_for_in_with_subqueries = 0,
        enable_multiple_prewhere_read_steps = 0,
        allow_reorder_prewhere_conditions = 0,
        short_circuit_function_evaluation = 'disable'
);

-- With one PREWHERE step and no lazy evaluation, the old separate `throwIf`
-- also ran on the unselected oversized row and raised an exception.
SELECT count() = 1 AND sum(length(samples)) = 2
FROM prometheusQueryRange(
    promql_native_rate_raw_guard,
    'rate(reads{instance="selected"}[20s])',
    20, 30, 10)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 1,
    max_promql_native_rate_samples_per_series = 4,
    use_index_for_in_with_subqueries = 0,
    enable_multiple_prewhere_read_steps = 0,
    allow_reorder_prewhere_conditions = 0,
    short_circuit_function_evaluation = 'disable';

-- Exercise the multi-step/reordered PREWHERE path as well.
SELECT count() = 1 AND sum(length(samples)) = 2
FROM prometheusQueryRange(
    promql_native_rate_raw_guard,
    'rate(reads{instance="selected"}[20s])',
    20, 30, 10)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 1,
    max_promql_native_rate_samples_per_series = 4,
    use_index_for_in_with_subqueries = 0,
    enable_multiple_prewhere_read_steps = 1,
    allow_reorder_prewhere_conditions = 1,
    short_circuit_function_evaluation = 'force_enable';

-- Selection must still fail closed when the oversized physical row is selected.
SELECT count()
FROM prometheusQueryRange(
    promql_native_rate_raw_guard,
    'rate(reads{instance="oversized"}[20s])',
    20, 30, 10)
SETTINGS
    enable_promql_native_plan = 1,
    enable_promql_native_raw_samples = 1,
    max_promql_native_rate_samples_per_series = 4,
    use_index_for_in_with_subqueries = 0,
    enable_multiple_prewhere_read_steps = 0,
    allow_reorder_prewhere_conditions = 0,
    short_circuit_function_evaluation = 'disable'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

DROP TABLE promql_native_rate_raw_guard;
