-- Tags: no-fasttest
-- PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS promql_count_subquery;

CREATE TABLE promql_count_subquery
(
    time_series Array(Tuple(DateTime64(3, 'UTC'), Float32))
)
ENGINE = TimeSeries;

INSERT INTO promql_count_subquery (metric_name, tags, time_series) VALUES
    ('m', map('host', 'h1'), [(toDateTime64('2025-11-30 10:30:05.125', 3, 'UTC'), 7)]);

-- `count` produces a `UInt64` grid via `countForEach`; the subquery feeds it into a `timeSeries*ToGrid` aggregate,
-- which accepts only floats. Subquery steps: 10:30:05 (no sample yet) and 10:30:10 (one series), so the sum is 1.
SELECT toTypeName(value), value
FROM prometheusQuery(
    'promql_count_subquery',
    'sum_over_time(count(m)[10:5])',
    toDateTime64('2025-11-30 10:30:10.250', 3, 'UTC'));

DROP TABLE promql_count_subquery;
