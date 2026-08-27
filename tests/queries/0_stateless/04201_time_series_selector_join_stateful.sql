-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables
-- synchronously (unlike for `MaterializedView`), so the deferred inner DROPs are rejected with
-- "It's not initial query. ON CLUSTER is not allowed for Replicated database.".

-- Regression test for the PromQL binary-operator path where ClickHouse query optimization
-- could push `timeSeriesIdToGroup(id)` ahead of the matching `timeSeriesStoreTags(...)` call,
-- resulting in `BAD_ARGUMENTS` ("Unknown identifier"). Fixed by marking
-- `timeSeriesIdToGroup` as stateful so the optimizer doesn't move it across pipeline barriers.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS prometheus;
DROP TABLE IF EXISTS tags_table;
DROP TABLE IF EXISTS samples_table;

-- We create a TimeSeries table `prometheus` with external target tables `tags_table`
-- and `samples_table`.
CREATE TABLE tags_table
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE samples_table
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE prometheus (id UInt64) ENGINE = TimeSeries
DATA samples_table TAGS tags_table;

-- Three metrics: `foo`, `bar`, `baz`.
INSERT INTO tags_table (id, metric_name, tags, min_time, max_time) VALUES
    (1583154, 'foo', map(), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (2836623, 'bar', map(), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (3691271, 'baz', map(), toDateTime64(0, 3), toDateTime64(1000, 3));

INSERT INTO samples_table (id, timestamp, value) VALUES
    (1583154, toDateTime64(100, 3), 10.),
    (2836623, toDateTime64(100, 3), 10.),
    (3691271, toDateTime64(100, 3), 20.);


-- PromQL query `(foo == bar)[50:10]` internally evaluates a SQL query like this:
--
-- SELECT timeSeriesGroupToTags(foo.group) AS tags, foo.timestamp, foo.value
-- FROM
-- (
--     SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
--     FROM samples_table
--     WHERE id IN (
--         SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name)
--         FROM tags_table
--         WHERE metric_name = 'foo' AND max_time >= toDateTime64(60, 3) AND min_time <= toDateTime64(150, 3))
--       AND timestamp >= toDateTime64(60, 3) AND timestamp <= toDateTime64(150, 3)
-- ) AS foo
-- ANY INNER JOIN
-- (
--     SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
--     FROM samples_table
--     WHERE id IN (
--         SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name)
--         FROM tags_table
--         WHERE metric_name = 'bar' AND max_time >= toDateTime64(60, 3) AND min_time <= toDateTime64(150, 3))
--       AND timestamp >= toDateTime64(60, 3) AND timestamp <= toDateTime64(150, 3)
-- ) AS bar
-- ON timeSeriesRemoveTag(foo.group, '__name__') = timeSeriesRemoveTag(bar.group, '__name__')
--    AND foo.timestamp = bar.timestamp AND foo.value = bar.value
-- ORDER BY tags;

SELECT * FROM prometheusQuery('prometheus', '(foo == bar)[50:10]', toDateTime64(150, 3));


DROP TABLE prometheus;
DROP TABLE tags_table;
DROP TABLE samples_table;
