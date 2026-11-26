-- Tags: no-fasttest
-- PromQL needs ANTLR4, which is disabled in the fast-test build.

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;
SET session_timezone = 'UTC';
-- A fuzzed rerun inherits log_comment and can hide the original error text.
SET ast_fuzzer_runs = 0;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

SELECT * FROM prometheusQuery(ts, 'start()', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'end()', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'sort_by_label(up, "job")', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'sort_by_label_desc(up, "job")', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'double_exponential_smoothing(up[5m], 0.01, 0.1)', 100); -- { serverError NOT_IMPLEMENTED }
SELECT * FROM prometheusQuery(ts, 'limit_ratio(0.5, up)', 100) SETTINGS log_comment = '05317_limit_ratio'; -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SYSTEM FLUSH LOGS query_log;

SELECT throwIf(count() = 0, 'Aggregation operator limit_ratio must say that it is not implemented')
FROM system.query_log
WHERE event_date >= yesterday()
    AND current_database = currentDatabase()
    AND log_comment = '05317_limit_ratio'
    AND positionCaseInsensitive(exception, 'not implemented') > 0
FORMAT Null;

DROP TABLE ts;
