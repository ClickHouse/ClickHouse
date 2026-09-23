-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- The left side of `or` is a materialized CTE, and its per-group presence mask is read twice. If the presence
-- mask were a materialized CTE too (a materialized CTE reading another materialized CTE), a query over
-- `prometheusQueryRange` with `LIMIT 0` could read it before its materialization completed. Found by the AST fuzzer.

DROP TABLE IF EXISTS prometheus;
DROP TABLE IF EXISTS tags_table;
DROP TABLE IF EXISTS samples_table;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

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

CREATE TABLE prometheus ENGINE = TimeSeries SAMPLES samples_table TAGS tags_table;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 2), (toDateTime64(130, 3), 3)]),
    ('m', map('host', 'h2'), [(toDateTime64(100, 3), 4), (toDateTime64(120, 3), 5)]),
    ('n', map('host', 'h1'), [(toDateTime64(100, 3), 7), (toDateTime64(110, 3), 7), (toDateTime64(120, 3), 7), (toDateTime64(130, 3), 7)]),
    ('n', map('host', 'h3'), [(toDateTime64(100, 3), 8), (toDateTime64(130, 3), 8)]);

SELECT count() FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10) LIMIT 0;
SELECT count() FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10) LIMIT 1, 0;
SELECT count() FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10);
SELECT count() FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or on() last_over_time(n[10])', 100, 130, 10);
SELECT count() FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) unless last_over_time(n[10])', 100, 130, 10) LIMIT 0;
SELECT count() FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) and last_over_time(n[10])', 100, 130, 10) LIMIT 0;
SELECT tags, samples FROM prometheusQueryRange('prometheus', 'last_over_time(m[10]) or last_over_time(n[10])', 100, 130, 10) ORDER BY tags;

DROP TABLE prometheus;
DROP TABLE tags_table;
DROP TABLE samples_table;
