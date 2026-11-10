-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Series which get the same tags (after dropping the metric name, `label_replace` or matching by `on`/`ignoring`) are duplicates
-- only if they have values at the same step, otherwise they are merged into one series.
-- With `group_left(labels)` each step takes the tags of the series matched at that step.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- The steps are 100 and 200, every range below is [50s], so a sample is visible only at its own step.
INSERT INTO ts (metric_name, tags, samples) VALUES
    ('a', map('dc', 'x', 'host', '1'), [(toDateTime64(100, 3), 1)]),
    ('a', map('dc', 'x', 'host', '2'), [(toDateTime64(200, 3), 2)]),
    ('b', map('dc', 'x', 'host', '1'), [(toDateTime64(100, 3), 10)]),
    ('b', map('dc', 'x', 'host', '2'), [(toDateTime64(100, 3), 20)]),
    ('c', map('dc', 'x'), [(toDateTime64(100, 3), 100), (toDateTime64(200, 3), 200)]),
    ('d', map('dc', 'x'), [(toDateTime64(100, 3), 5)]),
    ('e', map('dc', 'x', 'host', '1', 'team', 't'), [(toDateTime64(100, 3), 7)]),
    ('e', map('dc', 'x', 'host', '2', 'team', 't'), [(toDateTime64(200, 3), 8)]);

SELECT '-- dropping the metric name merges series with values at different steps';
SELECT * FROM prometheusQueryRange(ts, 'abs(last_over_time({__name__=~"a|b", host="2"}[50s]))', 100, 200, 100) ORDER BY ALL;

SELECT '-- dropping the metric name still reports series with values at the same step';
SELECT * FROM prometheusQueryRange(ts, 'abs(last_over_time({__name__=~"a|b", host="1"}[50s]))', 100, 200, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- a series which `and` keeps only at steps where it has no value is not a duplicate';
SELECT * FROM prometheusQueryRange(ts, 'abs(last_over_time({__name__=~"a|b", host="2"}[50s]) and on(dc) last_over_time(d[50s]))', 100, 200, 100) ORDER BY ALL;

SELECT '-- label_replace merges series with values at different steps';
SELECT * FROM prometheusQueryRange(ts, 'label_replace(last_over_time(a[50s]), "host", "all", "host", ".+")', 100, 200, 100) ORDER BY ALL;

SELECT '-- label_replace still reports series with values at the same step';
SELECT * FROM prometheusQueryRange(ts, 'label_replace(last_over_time(b[50s]), "host", "all", "host", ".+")', 100, 200, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- group_left copies labels from the series of the side "one" matched at each step';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(c[50s]) * on(dc) group_left(host) last_over_time(a[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- group_right copies labels from the series of the side "one" matched at each step';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(a[50s]) * on(dc) group_right(host) last_over_time(c[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- series of the side "one" matched at different steps give one result series if the copied labels are the same';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(c[50s]) * on(dc) group_left(team) last_over_time(e[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- series of the side "one" with the same copied labels still report values at the same step';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(c[50s]) * ignoring(host) group_left(dc) last_over_time(b[50s])', 100, 200, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- series of the side "one" with different copied labels still report values at the same step';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(c[50s]) * on(dc) group_left(host) last_over_time(b[50s])', 100, 200, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- group_left merges series of the side "many" which get the same tags at different steps';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time({__name__=~"a|b", host="2"}[50s]) * on(dc) group_left last_over_time(c[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- group_left still reports series of the side "many" which get the same tags at the same step';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time({__name__=~"a|b", host="1"}[50s]) * on(dc) group_left last_over_time(c[50s])', 100, 200, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE ts;
