-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The PromQL date functions (`hour`, `year`, ...) return NaN for a NaN or infinite sample, as Prometheus does.
-- Time steps without a value (NULLs in the grid) are kept as is.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

CREATE TABLE ts ENGINE = TimeSeries;

-- The two samples of `up{job="j"}` are one hour apart, which is longer than the default 5 minute lookback window.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', {'job': 'j'}, [(toDateTime64(0, 3, 'UTC'), 0), (toDateTime64(3600, 3, 'UTC'), 3600)]),
    ('up', {'job': 'nan'}, [(toDateTime64(0, 3, 'UTC'), nan)]);

SELECT '-- Finite values';
SELECT value FROM prometheusQuery(ts, 'hour(vector(0))', 0);
SELECT value FROM prometheusQuery(ts, 'hour(vector(3600))', 0);
SELECT value FROM prometheusQuery(ts, 'year(vector(0))', 0);
SELECT value FROM prometheusQuery(ts, 'hour()', 7200);
SELECT tags, value FROM prometheusQuery(ts, 'hour(up{job="j"})', 3600);

SELECT '-- NaN and infinite values';
SELECT value FROM prometheusQuery(ts, 'hour(vector(NaN))', 0);
SELECT value FROM prometheusQuery(ts, 'hour(vector(Inf))', 0);
SELECT value FROM prometheusQuery(ts, 'hour(vector(-Inf))', 0);
SELECT value FROM prometheusQuery(ts, 'year(vector(NaN))', 0);
SELECT value FROM prometheusQuery(ts, 'day_of_week(vector(Inf))', 0);
SELECT tags, value FROM prometheusQuery(ts, 'hour(up{job="nan"})', 0);

SELECT '-- Time steps without a value stay NULL';
SELECT tags, samples FROM prometheusQueryRange(ts, 'hour(up{job="j"})', 0, 3600, 1800);
SELECT tags, samples FROM prometheusQueryRange(ts, 'year(up)', 0, 3600, 1800) ORDER BY tags;

SELECT '-- Without short-circuit evaluation';
SET short_circuit_function_evaluation = 'disable';
SELECT tags, value FROM prometheusQuery(ts, 'hour(up)', 0) ORDER BY tags;
SELECT tags, samples FROM prometheusQueryRange(ts, 'year(up)', 0, 3600, 1800) ORDER BY tags;

DROP TABLE ts;
