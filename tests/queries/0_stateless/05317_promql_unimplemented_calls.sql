-- Tags: no-fasttest
-- PromQL needs ANTLR4, which is disabled in the fast-test build.

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

SELECT * FROM prometheusQuery(ts, 'start()', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'end()', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'step()', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'range()', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'sort_by_label(up, "job")', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'sort_by_label_desc(up, "job")', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'double_exponential_smoothing(up[5m], 0.01, 0.1)', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'limit_ratio(0.5, up)', 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE ts;
