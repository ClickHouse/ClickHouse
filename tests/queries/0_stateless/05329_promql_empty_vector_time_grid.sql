-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- clamp(up, 1, -1) is known to be empty when the query is built.
-- Expressions over it must still use the evaluation time grid.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', map('instance', 'host1'), [(toDateTime64(1700000000, 3), 1.0)]);

SELECT 'instant: (0 < clamp(up, 1, -1)) or vector(7)';
SELECT tags, toUnixTimestamp64Second(timestamp), value FROM prometheusQuery(ts, '(0 < clamp(up, 1, -1)) or vector(7)', 1700000000);

SELECT 'range: (0 < clamp(up, 1, -1)) or vector(7)';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples)
FROM prometheusQueryRange(ts, '(0 < clamp(up, 1, -1)) or vector(7)', 1700000000, 1700000060, 30);

SELECT 'range: (clamp(up, 1, -1) > up) or vector(7)';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples)
FROM prometheusQueryRange(ts, '(clamp(up, 1, -1) > up) or vector(7)', 1700000000, 1700000060, 30);

SELECT 'range: histogram_quantile(0.5, clamp(up, 1, -1)) or vector(7)';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples)
FROM prometheusQueryRange(ts, 'histogram_quantile(0.5, clamp(up, 1, -1)) or vector(7)', 1700000000, 1700000060, 30);

SELECT 'instant: scalar(0 < clamp(up, 1, -1))';
SELECT toUnixTimestamp64Second(timestamp), value FROM prometheusQuery(ts, 'scalar(0 < clamp(up, 1, -1))', 1700000000);

SELECT 'range: scalar(0 < clamp(up, 1, -1))';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples)
FROM prometheusQueryRange(ts, 'scalar(0 < clamp(up, 1, -1))', 1700000000, 1700000060, 30);

DROP TABLE ts;
