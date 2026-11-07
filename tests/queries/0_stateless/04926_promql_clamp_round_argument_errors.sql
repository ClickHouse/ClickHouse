-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Argument validation of the PromQL functions clamp/clamp_min/clamp_max/round.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1.5), (toDateTime64(110, 3), 2.5)]);

SELECT '-- wrong number of arguments';
SELECT * FROM prometheusQuery('prometheus', 'clamp(m, 0)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'clamp(m, 0, 1, 2)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'clamp_min(m)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'clamp_max(m, 0, 1)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'round(m, 1, 2)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- first argument must be an instant vector';
SELECT * FROM prometheusQuery('prometheus', 'clamp(1, 0, 2)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'clamp_min(1, 0)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'round(1)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- bounds must be scalars';
SELECT * FROM prometheusQuery('prometheus', 'clamp(m, m, 2)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'clamp(m, 0, m)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'clamp_max(m, m)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('prometheus', 'round(m, m)', 110); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- valid calls still work';
SELECT * FROM prometheusQuery('prometheus', 'clamp(m, 0, 2)', 110) ORDER BY tags;
SELECT * FROM prometheusQuery('prometheus', 'round(m)', 110) ORDER BY tags;

DROP TABLE prometheus;
