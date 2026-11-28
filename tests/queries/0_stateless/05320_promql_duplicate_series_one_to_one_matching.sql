-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Series which get the same tags after matching by `on`/`ignoring` are duplicates only if they have values at the same step
-- and the matched series of the other side has a value there too, otherwise they are merged into one series.

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
    ('f', map('dc', 'x'), [(toDateTime64(200, 3), 1000)]),
    ('g', map('dc', 'y'), [(toDateTime64(100, 3), 1)]);

SELECT '-- one-to-one matching merges series with values at different steps';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(a[50s]) + on(dc) last_over_time(c[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- one-to-one matching still reports series with values at the same step';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(b[50s]) + on(dc) last_over_time(c[50s])', 100, 200, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- the same for series of the right side';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(a[50s]) + on(dc) last_over_time(b[50s])', 100, 200, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- duplicates on the right side do not matter at steps where the matched left series has no value';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time({__name__=~"f|g"}[50s]) + on(dc) last_over_time({__name__=~"a|b"}[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- duplicates on the left side do not matter at steps where the matched right series has no value';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time({__name__=~"a|b"}[50s]) + on(dc) last_over_time({__name__=~"f|g"}[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- the same when the left side keeps the metric name';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time({__name__=~"a|b"}[50s]) < ignoring(host) last_over_time(f[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- a comparison keeps the metric name of its left side, series which get the same tags at different steps are still merged';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(a[50s]) < ignoring(host) last_over_time(c[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- a comparison with `on` keeps only the listed labels';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time(a[50s]) < on(dc) last_over_time(c[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- series with different metric names give different result series';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time({__name__=~"a|b", host="2"}[50s]) < ignoring(host) last_over_time(c[50s])', 100, 200, 100) ORDER BY ALL;

SELECT '-- series with different metric names are still duplicates if they have values at the same step';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time({__name__=~"a|b", host="1"}[50s]) < ignoring(host) last_over_time(c[50s])', 100, 200, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- series of the left side with values at the same step are duplicates even if the comparison keeps only one of them';
SELECT * FROM prometheusQueryRange(ts, 'last_over_time({__name__=~"a|b", host="1"}[50s]) > ignoring(host) last_over_time(d[50s])', 100, 200, 100); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

SELECT '-- a series without values on the left side of a comparison is skipped';
SELECT * FROM prometheusQueryRange(ts, '(last_over_time(a[50s]) and last_over_time(b[50s])) < ignoring(host) last_over_time(d[50s])', 100, 200, 100) ORDER BY ALL;

DROP TABLE ts;
