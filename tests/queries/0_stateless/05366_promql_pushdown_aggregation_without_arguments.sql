-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET promql_push_down_label_matchers = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

SELECT * FROM prometheusQueryRange('ts', 'sum() / on(job) up{job="a"}', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange('ts', 'up{job="a"} / on(job) topk by (job) ()', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange('ts', 'quantile() and on(job) up{job="a"}', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange('ts', 'up{job="a"} unless on(job) sum by (job) (sum())', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQueryRange('ts', 'max_over_time(sum()[30s:10s]) / on(job) up{job="a"}', 100, 130, 10); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE ts;
