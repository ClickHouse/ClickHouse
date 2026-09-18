-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Tests the PromQL functions `present_over_time`, `absent_over_time` and `absent`, which are built on the aggregate
-- function timeSeriesPresentToGrid (tested in 05210_timeseries_present_to_grid).

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- The evaluation time is 1700000000. host1, host2 and host3 have samples in the [3m] window (1699999820, 1700000000],
-- host4 has a single older sample. up2/host1 differs from up/host1 only by the metric name.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('up', map('instance', 'host1'), [(toDateTime64(1699999880, 3), 1.0), (toDateTime64(1699999940, 3), 1.0), (toDateTime64(1700000000, 3), 1.0)]),
    ('up', map('instance', 'host2'), [(toDateTime64(1699999880, 3), 1.0), (toDateTime64(1699999940, 3), 1.0), (toDateTime64(1700000000, 3), 1.0)]),
    ('up', map('instance', 'host3'), [(toDateTime64(1699999880, 3), 1.0), (toDateTime64(1699999940, 3), 1.0), (toDateTime64(1700000000, 3), 1.0)]),
    ('up', map('instance', 'host4'), [(toDateTime64(1699999000, 3), 7.0)]),
    ('up2', map('instance', 'host1'), [(toDateTime64(1700000000, 3), 1.0)]);

SELECT 'present_over_time: 1 for every series with a sample in the window, the stale host4 is dropped';
SELECT tags, value FROM prometheusQuery(ts, 'present_over_time(up[3m])', 1700000000) ORDER BY ALL;

-- The last sample is at 1700000000, so the series are present at the steps 1700000000 and 1700000200 (the 5-minute
-- range still reaches the samples) and absent at the steps 1700000400 and 1700000600.
SELECT 'present_over_time, range query: only the steps whose window has samples are returned';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) AS samples
FROM prometheusQueryRange(ts, 'present_over_time(up[5m])', 1700000000, 1700000600, 200) ORDER BY ALL;

SELECT 'absent_over_time: the metric has samples in the window, so the result is empty';
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time(up[3m])', 1700000000);

SELECT 'absent_over_time: no samples anywhere, a single synthetic series without labels';
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time(nonexistent_metric[5m])', 1700000000);

SELECT 'absent_over_time: series differing only by the metric name do not clash in the presence grid';
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time({__name__!="",instance="host1"}[3m])', 1700000000);

SELECT 'absent_over_time: no series matches, the labels come from the equality matchers';
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time(up{instance="nohost"}[5m])', 1700000000);
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time(nonexistent{job="api"}[5m])', 1700000000);

SELECT 'absent_over_time: a subquery never infers labels, Prometheus infers them from selectors only';
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time(nonexistent_metric{instance="host1"}[5m:1m])', 1700000000);

SELECT 'absent_over_time, range query: the synthetic series appears only at the steps where the real series is gone';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) AS samples
FROM prometheusQueryRange(ts, 'absent_over_time(up[5m])', 1700000000, 1700000600, 200);

SELECT 'absent: no series matches, the labels come from the equality matchers';
SELECT tags, value FROM prometheusQuery(ts, 'absent(up{instance="nohost"})', 1700000000);
SELECT tags, value FROM prometheusQuery(ts, 'absent(nonexistent{job="api"})', 1700000000);

SELECT 'absent: a series is present, so the result is empty';
SELECT tags, value FROM prometheusQuery(ts, 'absent(up)', 1700000000);

SELECT 'absent, range query: the synthetic series appears only at the steps where the real series is gone';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) AS samples
FROM prometheusQueryRange(ts, 'absent(up)', 1700000000, 1700000600, 200);

-- `absent` and `absent_over_time` count the present series with an aggregation without keys. Such an aggregation
-- over an empty input returns no rows when `empty_result_for_aggregation_by_empty_set` is enabled, but an empty
-- input is exactly the case when these functions must produce their synthetic series. The translator adds a neutral
-- row to the aggregated input, so the result must not depend on the setting.
SET empty_result_for_aggregation_by_empty_set = 1;

SELECT 'empty_result_for_aggregation_by_empty_set does not change the results';
SELECT tags, value FROM prometheusQuery(ts, 'absent(up{instance="nohost"})', 1700000000);
SELECT tags, value FROM prometheusQuery(ts, 'absent(up)', 1700000000);
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time(nonexistent{job="api"}[5m])', 1700000000);
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time(up[5m])', 1700000000);
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) AS samples
FROM prometheusQueryRange(ts, 'absent(up)', 1700000000, 1700000600, 200);
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), samples) AS samples
FROM prometheusQueryRange(ts, 'absent_over_time(up[5m])', 1700000000, 1700000600, 200);

DROP TABLE ts;
