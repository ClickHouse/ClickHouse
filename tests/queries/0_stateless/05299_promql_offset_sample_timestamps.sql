-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- An offset on a range selector or a subquery keeps the timestamps of the samples, as in Prometheus.

SET allow_experimental_time_series_table = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- The value of every sample is its number of seconds since 1000000.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'lin', map('s', '0'), arrayMap(k -> (toDateTime64(1000000 + k * 15, 3), toFloat64(k * 15)), range(60));

SELECT 'ts_of_last_over_time(lin[1m] offset 1m)', value FROM prometheusQuery(ts, 'ts_of_last_over_time(lin[1m] offset 1m)', 1000600);
SELECT 'ts_of_first_over_time(lin[1m] offset 1m)', value FROM prometheusQuery(ts, 'ts_of_first_over_time(lin[1m] offset 1m)', 1000600);
SELECT 'ts_of_max_over_time(lin[1m] offset 1m)', value FROM prometheusQuery(ts, 'ts_of_max_over_time(lin[1m] offset 1m)', 1000600);
SELECT 'ts_of_last_over_time(lin[1m] offset -1m)', value FROM prometheusQuery(ts, 'ts_of_last_over_time(lin[1m] offset -1m)', 1000600);
SELECT 'ts_of_last_over_time(lin[1m] offset 1m) range', arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(ts, 'ts_of_last_over_time(lin[1m] offset 1m)', 1000540, 1000600, 30);

SELECT 'lin[1m] offset 1m', arrayMap(x -> (toFloat64(x.1), x.2), samples) FROM prometheusQuery(ts, 'lin[1m] offset 1m', 1000600);
SELECT 'lin[30s] offset -1m', arrayMap(x -> (toFloat64(x.1), x.2), samples) FROM prometheusQuery(ts, 'lin[30s] offset -1m', 1000600);
SELECT 'lin[1m] offset 500ms', arrayMap(x -> (toFloat64(x.1), x.2), samples) FROM prometheusQuery(ts, 'lin[1m] offset 500ms', 1000600);

SELECT 'lin[1m:15s] offset 1m', arrayMap(x -> (toFloat64(x.1), x.2), samples) FROM prometheusQuery(ts, 'lin[1m:15s] offset 1m', 1000600);
SELECT 'ts_of_last_over_time(lin[1m:15s] offset 1m)', value FROM prometheusQuery(ts, 'ts_of_last_over_time(lin[1m:15s] offset 1m)', 1000600);

SELECT 'predict_linear(lin[2m] offset 1m, 60)', value FROM prometheusQuery(ts, 'predict_linear(lin[2m] offset 1m, 60)', 1000600);
SELECT 'predict_linear(lin[2m:15s] offset 1m, 60)', value FROM prometheusQuery(ts, 'predict_linear(lin[2m:15s] offset 1m, 60)', 1000600);
SELECT 'predict_linear(lin[2m] offset 1m, 60) range', arrayMap(x -> x.2, samples)
FROM prometheusQueryRange(ts, 'predict_linear(lin[2m] offset 1m, 60)', 1000540, 1000600, 30);

DROP TABLE ts;
