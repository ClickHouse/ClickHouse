-- Reproduces the failing pattern for PromQL binary operators because of that
-- ClickHouse optimization reorders things so that `timeSeriesIdToGroup(id)`
-- is evaluated for an `id` that `timeSeriesStoreTags(...)` has not yet recorded.

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


-- Here PromQL query `(foo == bar)[50:10]` fails because ClickHouse query optimization reorders things
-- so that `timeSeriesIdToGroup(id)` is evaluated for an `id` that `timeSeriesStoreTags(...)`
-- has not yet recorded, and the lookup fails with `BAD_ARGUMENTS` ("Unknown identifier") inside
-- `MergeTreeSelect(pool: ReadPoolInOrder, algorithm: InOrder)`.
SELECT * FROM prometheusQuery('prometheus', '(foo == bar)[50:10]', toDateTime64(150, 3)); -- { serverError BAD_ARGUMENTS }

-- The query above `(foo == bar)[50:10]` is equivalent to the following SQL query which also fails:
SELECT timeSeriesGroupToTags(foo.group) AS tags, foo.timestamp, foo.value
FROM
(
    SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
    FROM samples_table
    WHERE id IN (
        SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name)
        FROM tags_table
        WHERE metric_name = 'foo' AND max_time >= toDateTime64(60, 3) AND min_time <= toDateTime64(150, 3))
      AND timestamp >= toDateTime64(60, 3) AND timestamp <= toDateTime64(150, 3)
) AS foo
ANY INNER JOIN
(
    SELECT timeSeriesIdToGroup(id) AS group, timestamp, value
    FROM samples_table
    WHERE id IN (
        SELECT timeSeriesStoreTags(id, tags, '__name__', metric_name)
        FROM tags_table
        WHERE metric_name = 'bar' AND max_time >= toDateTime64(60, 3) AND min_time <= toDateTime64(150, 3))
      AND timestamp >= toDateTime64(60, 3) AND timestamp <= toDateTime64(150, 3)
) AS bar
ON timeSeriesRemoveTag(foo.group, '__name__') = timeSeriesRemoveTag(bar.group, '__name__')
   AND foo.timestamp = bar.timestamp AND foo.value = bar.value
ORDER BY tags; -- { serverError BAD_ARGUMENTS }


DROP TABLE prometheus;
DROP TABLE tags_table;
DROP TABLE samples_table;
