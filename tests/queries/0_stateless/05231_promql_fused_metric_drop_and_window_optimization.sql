-- Tags: no-fasttest, no-parallel-replicas
-- Tests fused metric drop optimization and subquery elimination in PromQL range queries.

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS tags_table;
DROP TABLE IF EXISTS samples_table;
DROP TABLE IF EXISTS prometheus;

CREATE TABLE tags_table
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE samples_table
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE prometheus ENGINE = TimeSeries
SAMPLES samples_table TAGS tags_table;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1.0), (toDateTime64(110, 3), 2.0), (toDateTime64(120, 3), 4.0), (toDateTime64(130, 3), 8.0)]),
    ('m', map('host', 'h2'), [(toDateTime64(100, 3), 10.0), (toDateTime64(110, 3), 20.0), (toDateTime64(120, 3), 40.0), (toDateTime64(130, 3), 80.0)]),
    ('n', map('host', 'h1'), [(toDateTime64(100, 3), 5.0), (toDateTime64(110, 3), 15.0), (toDateTime64(120, 3), 25.0), (toDateTime64(130, 3), 35.0)]);

SELECT '-- exact metric name rate range query';
SELECT tags, samples FROM prometheusQueryRange('prometheus', 'rate(m[20])', 100, 130, 10) ORDER BY ALL;

SELECT '-- exact metric name increase range query';
SELECT tags, samples FROM prometheusQueryRange('prometheus', 'increase(m[20])', 100, 130, 10) ORDER BY ALL;

SELECT '-- non-exact metric selector with duplicate series collision throws exception';
SELECT count() FROM prometheusQueryRange('prometheus', 'rate({host="h1"}[20])', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- explain plan verifies single aggregation step when metric name is exact';
SELECT countIf(explain LIKE '%Aggregating%') AS aggregating_steps,
       countIf(explain LIKE '%any(timeSeriesRateToGrid%') AS has_redundant_array_aggregation
FROM (EXPLAIN SELECT * FROM prometheusQueryRange('prometheus', 'rate(m[20])', 100, 130, 10));

DROP TABLE prometheus;
DROP TABLE tags_table;
DROP TABLE samples_table;
