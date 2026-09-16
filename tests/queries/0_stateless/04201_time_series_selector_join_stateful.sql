-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Regression test for the PromQL binary-operator path where ClickHouse query optimization
-- could push `timeSeriesIdToGroup(id)` ahead of the matching `timeSeriesStoreTags(...)` call,
-- resulting in `BAD_ARGUMENTS` ("Unknown identifier"). Fixed by marking
-- `timeSeriesIdToGroup` as stateful so the optimizer doesn't move it across pipeline barriers.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS prometheus;

CREATE TABLE prometheus ENGINE = TimeSeries;

-- Three metrics: `foo`, `bar`, `baz`, each with one sample at the same timestamp.
INSERT INTO prometheus (metric_name, tags, samples) VALUES
    ('foo', map(), [(toDateTime64(100, 3), 10.)]),
    ('bar', map(), [(toDateTime64(100, 3), 10.)]),
    ('baz', map(), [(toDateTime64(100, 3), 20.)]);

-- PromQL query `(foo == bar)[50:10]` internally evaluates a SQL query like this, where `samples_table`,
-- `tags_table` and `time_ranges_table` stand for the inner target tables of `prometheus`:
--
-- SELECT timeSeriesGroupToTags(foo.group) AS tags, foo.timestamp, foo.value
-- FROM
-- (
--     SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
--     FROM samples_table
--     WHERE id IN (
--         SELECT id FROM time_ranges_table
--         WHERE id IN (SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name) FROM tags_table WHERE metric_name = 'foo')
--           AND max_time >= toDateTime64(60, 3) AND min_time <= toDateTime64(150, 3))
--       AND timestamp >= toDateTime64(60, 3) AND timestamp <= toDateTime64(150, 3)
-- ) AS foo
-- ANY INNER JOIN
-- (
--     SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
--     FROM samples_table
--     WHERE id IN (
--         SELECT id FROM time_ranges_table
--         WHERE id IN (SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name) FROM tags_table WHERE metric_name = 'bar')
--           AND max_time >= toDateTime64(60, 3) AND min_time <= toDateTime64(150, 3))
--       AND timestamp >= toDateTime64(60, 3) AND timestamp <= toDateTime64(150, 3)
-- ) AS bar
-- ON timeSeriesRemoveTag(foo.group, '__name__') = timeSeriesRemoveTag(bar.group, '__name__')
--    AND foo.timestamp = bar.timestamp AND foo.value = bar.value
-- ORDER BY tags;

SELECT * FROM prometheusQuery('prometheus', '(foo == bar)[50:10]', toDateTime64(150, 3));

DROP TABLE prometheus;
