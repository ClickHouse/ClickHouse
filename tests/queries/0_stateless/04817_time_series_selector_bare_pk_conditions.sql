-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `timeSeriesSelector` (and every PromQL selector evaluated through it) builds a SELECT over the
-- samples table. That inner SELECT must reference the bare `id` / `bucket` columns (no casts wrapping
-- the primary key columns) and must put the selective time range conditions (on `bucket`,
-- `min_time` and `max_time`) before the `id IN <tags subquery>` condition. Wrapped primary-key
-- columns are re-evaluated over the whole primary index during index analysis, and they disable
-- primary-key-based selectivity estimation - then the PREWHERE read steps are misordered and
-- the expensive `in(id, set)` probe runs on all read rows. The casts to the declared types are
-- applied by an outer SELECT over the filtered subquery, so they run only on the rows which
-- passed the filter.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_tags;
DROP TABLE IF EXISTS ts_samples;
DROP TABLE IF EXISTS ts;

CREATE TABLE ts_tags
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String),
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE ts_samples
(
    id UInt64,
    samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))),
    bucket DateTime64(3),
    min_time SimpleAggregateFunction(min, DateTime64(3)),
    max_time SimpleAggregateFunction(max, DateTime64(3))
) ENGINE = AggregatingMergeTree() ORDER BY (id, bucket);

CREATE TABLE ts ENGINE = TimeSeries SAMPLES ts_samples TAGS ts_tags;

-- Series 201 ('bar') must not match the 'foo' selector, and it has a sample inside the requested
-- time range - so if the `id IN <tags subquery>` condition is ever lost from the generated query,
-- that sample leaks into the result and the test fails.
INSERT INTO ts_tags (id, metric_name, tags, min_time, max_time) VALUES
    (101, 'foo', map('env', 'prod'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (102, 'foo', map('env', 'dev'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (201, 'bar', map(), toDateTime64(0, 3), toDateTime64(1000, 3));

-- All the samples belong to the bucket starting at 0 (the default bucket step is 1 hour).
INSERT INTO ts_samples (id, samples, bucket, min_time, max_time) VALUES
    (101, [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.), (toDateTime64(300, 3), 3.)], toDateTime(0), toDateTime64(100, 3), toDateTime64(300, 3)),
    (102, [(toDateTime64(150, 3), 10.), (toDateTime64(250, 3), 20.)], toDateTime(0), toDateTime64(150, 3), toDateTime64(250, 3)),
    (201, [(toDateTime64(100, 3), 100.), (toDateTime64(300, 3), 300.)], toDateTime(0), toDateTime64(100, 3), toDateTime64(300, 3));

SELECT '-- timeSeriesSelector returns the same samples as a direct filtered read of the samples table';

SELECT id, sample.1 AS timestamp, sample.2 AS value
FROM (SELECT id, arrayJoin(time_series) AS sample FROM timeSeriesSelector(ts, 'foo', 100, 250))
ORDER BY id, timestamp;

SELECT '(direct read for comparison)';

SELECT id, sample.1 AS timestamp, sample.2 AS value
FROM (SELECT id, arrayJoin(samples) AS sample FROM ts_samples WHERE id IN (101, 102))
WHERE (timestamp >= toDateTime64(100, 3)) AND (timestamp <= toDateTime64(250, 3))
ORDER BY id, timestamp;

SELECT '-- the SELECT over the samples table uses bare columns, time range first';

-- The WHERE conditions must reference the bare `id` / `bucket` columns in the generated order.
-- If aliased casts shadow the columns, the conditions render with the wrapped expressions,
-- and `position` returns 0 for both patterns.
-- `optimize_move_to_prewhere = 0` keeps the generated condition order.
SELECT position(plan, 'bucket >=') BETWEEN 1 AND position(plan, 'id IN') AS bare_bucket_condition_before_id_in,
       position(plan, 'max_time >=') BETWEEN 1 AND position(plan, 'id IN') AS max_time_condition_before_id_in
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, time_series FROM timeSeriesSelector(ts, 'foo', 100, 250) SETTINGS optimize_move_to_prewhere = 0));

SELECT '-- a samples table whose physical type differs (here: by timezone only): bare conditions, cast in the outer SELECT';

-- The declared timestamp type of `ts` was captured as `DateTime64(3)` when the table was created,
-- so this ALTER changes only the physical type of the samples-table columns.
ALTER TABLE ts_samples
    MODIFY COLUMN samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3, 'UTC'), value Float64))),
    MODIFY COLUMN min_time SimpleAggregateFunction(min, DateTime64(3, 'UTC')),
    MODIFY COLUMN max_time SimpleAggregateFunction(max, DateTime64(3, 'UTC'));

-- The types row of `TSVWithNamesAndTypes` shows the runtime type of the result.
SELECT id, time_series FROM timeSeriesSelector(ts, 'foo', 100, 250) ORDER BY id FORMAT TSVWithNamesAndTypes;

-- The cast of `time_series` to the declared type appears only in the outer SELECT (the `Output:`
-- line of the plan), not in the conditions: comparing the bare columns is correct
-- because the timezone does not change the stored values.
SELECT '-- the bare bucket condition comes before the id IN condition, and the time_series cast is in the outer SELECT';

SELECT position(plan, 'bucket >=') BETWEEN 1 AND position(plan, 'id IN') AS bare_bucket_condition_before_id_in,
       plan LIKE '%CAST(timeSeriesSliceSortedArray(%AS Array(Tuple(DateTime64(3), Float64)))%' AS time_series_cast_in_outer_select
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, time_series FROM timeSeriesSelector(ts, 'foo', 100, 250) SETTINGS optimize_move_to_prewhere = 0));

DROP TABLE ts;
DROP TABLE ts_samples;
DROP TABLE ts_tags;
