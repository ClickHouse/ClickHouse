-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Empty on() matches every series as one group. The join key is the constant group 0,
-- so each side of JOIN ON must be a column. A 5s range leaves steps 110 and 120 empty;
-- the 5m instant lookback would still see the sample at 100.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('m', map('pod', 'p1'), [(toDateTime64(100, 3), 1)]),
    ('m', map('pod', 'p2'), [(toDateTime64(100, 3), 2)]);

SELECT 'or keeps the left series when they are present';
SELECT tags, value
FROM prometheusQuery('prometheus', 'sum(m) by (pod) or on() vector(0)', 100)
ORDER BY tags;

SELECT 'or fills an empty left side with vector(0)';
SELECT tags, value
FROM prometheusQuery('prometheus', 'sum(nope) by (pod) or on() vector(0)', 100)
ORDER BY tags;

SELECT 'or range fills only the steps where the left side is absent';
SELECT tags, samples
FROM prometheusQueryRange(
    'prometheus',
    'sum by (pod) (last_over_time(m[5])) or on() vector(0)',
    100, 120, 10)
ORDER BY tags;

SELECT 'and keeps the left series when the right side is present';
SELECT tags, value
FROM prometheusQuery('prometheus', 'sum(m) by (pod) and on() vector(1)', 100)
ORDER BY tags;

SELECT 'and with an empty right side returns no series';
SELECT count()
FROM prometheusQuery('prometheus', 'sum(m) by (pod) and on() (vector(1) == 0)', 100);

SELECT 'unless drops the left series when the right side is present';
SELECT count()
FROM prometheusQuery('prometheus', 'sum(m) by (pod) unless on() vector(1)', 100);

SELECT 'unless keeps the left series when the right side is empty';
SELECT tags, value
FROM prometheusQuery('prometheus', 'sum(m) by (pod) unless on() (vector(1) == 0)', 100)
ORDER BY tags;

SELECT 'or drops the right side when the left side is present';
SELECT tags, value
FROM prometheusQuery('prometheus', 'vector(1) or on() sum(m) by (pod)', 100)
ORDER BY tags;

SELECT 'on(pod) still removes every tag except pod';
SELECT countIf(explain LIKE '%timeSeriesRemoveAllTagsExcept%') > 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'sum(m) by (pod) or on(pod) vector(0)', 100));

DROP TABLE prometheus;
