-- Tags: no-fasttest, no-replicated-database
-- PromQL needs ANTLR4, which is disabled in the fast-test build. The TimeSeries table uses external
-- data tables, whose cleanup is not synchronous in DatabaseReplicated.

SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS promql_count_group_f32;

CREATE TABLE promql_count_group_f32
(
    time_series Array(Tuple(DateTime64(3, 'UTC'), Float32))
)
ENGINE = TimeSeries;

INSERT INTO promql_count_group_f32 (metric_name, tags, time_series) VALUES
    ('m', map('host', 'h1'), [(toDateTime64('2025-11-30 10:30:05.125', 3, 'UTC'), 7)]);

-- count() synthesizes an exact series count, so its values are Float64 even on a Float32 table.
SELECT toTypeName(any(value)), any(value)
FROM prometheusQuery(
    'promql_count_group_f32',
    'count(m)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- group() synthesizes fresh scalar-typed samples, so the table's Float32 shows through...
SELECT toTypeName(any(value)), any(value)
FROM prometheusQuery(
    'promql_count_group_f32',
    'group(m)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- ...even when its argument carries count_over_time's Float64 override, which must not leak out.
SELECT toTypeName(any(value)), any(value)
FROM prometheusQuery(
    'promql_count_group_f32',
    'group(count_over_time(m[10]))',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- The count grid is really Float64, not just advertised as such: a nested composition feeds it
-- back into a timeSeries*ToGrid aggregate, which rejects an integer sample column.
SELECT toTypeName(any(value)), count() > 0
FROM prometheusQuery(
    'promql_count_group_f32',
    'sum_over_time(count(m)[10:5])',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- `and` / `unless` pass the left operand's values through, so its override survives them - and it
-- must not depend on whether the result happens to contain rows, since the schema shows through
-- either way (the second and third queries match nothing).
SELECT toTypeName(any(value))
FROM prometheusQuery(
    'promql_count_group_f32',
    'count(m) and vector(0)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

SELECT toTypeName(any(value))
FROM prometheusQuery(
    'promql_count_group_f32',
    'count(m) and m',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

SELECT toTypeName(any(value))
FROM prometheusQuery(
    'promql_count_group_f32',
    'count(m) unless vector(0)',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- `or` takes values from both sides, so the overrides merge rather than being dropped.
SELECT toTypeName(any(value))
FROM prometheusQuery(
    'promql_count_group_f32',
    'count(m) or m',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

-- Aggregation wrappers over a compile-time-empty argument must report the same type as over a
-- non-empty one. At :19.500 the `[19:20]` subquery aligns to start > end, so the range really is
-- empty and each wrapper takes its early-return path; `count_over_time`'s Float64 has to survive it.
SELECT toTypeName(any(value))
FROM prometheusQuery(
    'promql_count_group_f32',
    'sum(count_over_time(vector(1)[19:20]))',
    toDateTime64('2025-11-30 10:30:19.500', 3, 'UTC'));

SELECT toTypeName(any(value))
FROM prometheusQuery(
    'promql_count_group_f32',
    'quantile(0.5, count_over_time(vector(1)[19:20]))',
    toDateTime64('2025-11-30 10:30:19.500', 3, 'UTC'));

SELECT toTypeName(any(value))
FROM prometheusQuery(
    'promql_count_group_f32',
    'topk(1, count_over_time(vector(1)[19:20]))',
    toDateTime64('2025-11-30 10:30:19.500', 3, 'UTC'));

-- ...but `group` still synthesizes scalar-typed samples, so it must keep dropping the override.
SELECT toTypeName(any(value))
FROM prometheusQuery(
    'promql_count_group_f32',
    'group(count_over_time(vector(1)[19:20]))',
    toDateTime64('2025-11-30 10:30:19.500', 3, 'UTC'));

-- Calendar-component wrappers also synthesize samples in the table's scalar type, so a child's
-- Float64 override must not survive them either.
SELECT toTypeName(any(value))
FROM prometheusQuery(
    'promql_count_group_f32',
    'minute(count_over_time(m[10]))',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

DROP TABLE promql_count_group_f32;
