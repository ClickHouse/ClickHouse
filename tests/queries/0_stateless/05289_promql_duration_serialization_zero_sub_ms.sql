-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Zero and sub-millisecond PromQL durations: two aggregations whose arguments differ only by a sub-millisecond offset
-- are not calculated in a single aggregation, and the promql dialect accepts zero durations.

DROP TABLE IF EXISTS ts6;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE ts6 (samples Array(Tuple(DateTime64(6, 'UTC'), Float64))) ENGINE = TimeSeries;

-- The second sample is 0.3 ms before the evaluation time: `offset 0.0002` sees it, `offset 0.0004` does not.
INSERT INTO ts6 (metric_name, tags, samples) VALUES
    ('m', {'host': 'h1'}, [(toDateTime64(100, 6, 'UTC'), 1), (toDateTime64('1970-01-01 00:02:09.9997', 6, 'UTC'), 10)]);

SELECT * FROM prometheusQuery(ts6, 'sum(m offset 0.0002)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery(ts6, 'max(m offset 0.0004)', 130) ORDER BY tags;
SELECT * FROM prometheusQuery(ts6, 'sum(m offset 0.0002) - max(m offset 0.0004)', 130) ORDER BY tags;

SET promql_table = 'ts6';
SET promql_evaluation_time = 130;
SET dialect = 'promql';
m offset 0s;
vector(0s);
SET dialect = 'clickhouse';

DROP TABLE ts6;
