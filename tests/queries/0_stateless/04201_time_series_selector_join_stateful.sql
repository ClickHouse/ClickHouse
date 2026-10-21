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
--     SELECT timeSeriesIdToGroup(id) AS group, timeSeriesSliceSortedArray(samples, toDateTime64(60, 3), toDateTime64(150, 3)) AS time_series
--     FROM samples_table
--     WHERE bucket >= toDateTime64(0, 3) AND bucket <= toDateTime64(150, 3)
--       AND max_time >= toDateTime64(60, 3) AND min_time <= toDateTime64(150, 3)
--       AND id IN (
--         SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name) FROM tags_table
--         LEFT JOIN (SELECT id, 1 AS matched FROM time_ranges_table
--                    WHERE max_time >= toDateTime64(60, 3) AND min_time <= toDateTime64(150, 3) AND <id range of the metric 'foo'>) AS time_ranges_subquery USING (id)
--         WHERE metric_name = 'foo' AND (time_ranges_subquery.matched = 1 OR tupleElement(id, 1) != sipHash64('foo')))
-- ) AS foo
-- ANY INNER JOIN
-- (
--     SELECT timeSeriesIdToGroup(id) AS group, timeSeriesSliceSortedArray(samples, toDateTime64(60, 3), toDateTime64(150, 3)) AS time_series
--     FROM samples_table
--     WHERE bucket >= toDateTime64(0, 3) AND bucket <= toDateTime64(150, 3)
--       AND max_time >= toDateTime64(60, 3) AND min_time <= toDateTime64(150, 3)
--       AND id IN (
--         SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name) FROM tags_table
--         LEFT JOIN (SELECT id, 1 AS matched FROM time_ranges_table
--                    WHERE max_time >= toDateTime64(60, 3) AND min_time <= toDateTime64(150, 3) AND <id range of the metric 'bar'>) AS time_ranges_subquery USING (id)
--         WHERE metric_name = 'bar' AND (time_ranges_subquery.matched = 1 OR tupleElement(id, 1) != sipHash64('bar')))
-- ) AS bar
-- ON timeSeriesRemoveTag(foo.group, '__name__') = timeSeriesRemoveTag(bar.group, '__name__')
--    AND foo.timestamp = bar.timestamp AND foo.value = bar.value
-- ORDER BY tags;

SELECT * FROM prometheusQuery('prometheus', '(foo == bar)[50:10]', toDateTime64(150, 3));

DROP TABLE prometheus;
