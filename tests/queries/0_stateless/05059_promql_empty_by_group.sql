-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

DROP TABLE IF EXISTS prometheus;

SET session_timezone = 'UTC';
SET allow_experimental_time_series_table = 1;

CREATE TABLE prometheus ENGINE = TimeSeries;

INSERT INTO prometheus (metric_name, tags, time_series) VALUES
    ('m', map('host', 'h1'), [(toDateTime64(100, 3), 1)]),
    ('m', map('host', 'h2'), [(toDateTime64(100, 3), 2)]);

SELECT 'sum by() aggregates all series into one group';
SELECT count() AS series_count, sum(value) AS value
FROM prometheusQuery('prometheus', 'sum by() (m)', 100);

SELECT 'sum by() uses the constant empty group';
SELECT countIf(explain LIKE '%timeSeriesRemoveAllTagsExcept%') = 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'sum by() (m)', 100));

-- An empty `by()` always produces the single empty label set, and the empty set of tags is always group 0
-- (see `ContextTimeSeriesTagsCollector::getGroupForNoTags`), so the group expression is the constant group 0
-- instead of `timeSeriesRemoveAllTagsExcept(group, [])`. The transformation is shared by every kind of
-- aggregation operator, so a one-argument operator, `quantile` and a limit operator are all checked here.
SELECT 'sum by() aggregates all series into one group with no tags';
SELECT tags, value FROM prometheusQuery('prometheus', 'sum by() (m)', 100);

SELECT 'quantile by() aggregates all series into one group with no tags';
SELECT tags, value FROM prometheusQuery('prometheus', 'quantile by () (0.5, m)', 100);

SELECT 'topk by() selects over all series and keeps their own tags';
SELECT tags, value FROM prometheusQuery('prometheus', 'topk(1, m) by ()', 100) ORDER BY tags;

-- The `by (host)` query is a control: it must still transform the group, so the assertions above cannot
-- pass just because the plan text stops mentioning the function for an unrelated reason.
SELECT 'the group expression is the constant empty group, not timeSeriesRemoveAllTagsExcept';
SELECT 'sum by()', countIf(explain LIKE '%timeSeriesRemoveAllTagsExcept%') = 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'sum by() (m)', 100));
SELECT 'quantile by()', countIf(explain LIKE '%timeSeriesRemoveAllTagsExcept%') = 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'quantile by () (0.5, m)', 100));
SELECT 'topk by()', countIf(explain LIKE '%timeSeriesRemoveAllTagsExcept%') = 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'topk(1, m) by ()', 100));
SELECT 'by (host), control', countIf(explain LIKE '%timeSeriesRemoveAllTagsExcept%') > 0
FROM (EXPLAIN SELECT * FROM prometheusQuery('prometheus', 'sum by (host) (m)', 100));

DROP TABLE prometheus;
