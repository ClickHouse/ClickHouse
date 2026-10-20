-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: every parallel replica reads all rows of the table behind `timeSeriesSamples`, so each row
-- is returned once per replica until https://github.com/ClickHouse/ClickHouse/pull/119350 is merged.

-- `timeSeriesSelector` (and every PromQL selector evaluated through it) builds a SELECT over the
-- samples table. That inner SELECT must reference the bare `id` / `timestamp` columns (no casts wrapping
-- the primary key columns) and must put the selective `timestamp` conditions before the
-- `id IN <tags subquery>` condition. The tags subquery also filters the series by `min_time`
-- and `max_time`. Wrapped primary-key
-- columns are re-evaluated over the whole primary index during index analysis, and they disable
-- primary-key-based selectivity estimation - then the PREWHERE read steps are misordered and
-- the expensive `in(id, set)` probe runs on all read rows. The casts to the declared types are
-- applied by an outer SELECT over the filtered subquery, so they run only on the rows which
-- passed the filter.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

CREATE TABLE ts_tags
(
    id UInt64,
    metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE ts_time_ranges
(
    id UInt64,
    min_time DateTime64(3),
    max_time DateTime64(3)
) ENGINE = MergeTree() ORDER BY id;

CREATE TABLE ts_samples
(
    id UInt64,
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts ENGINE = TimeSeries SETTINGS version = 9
    SAMPLES ts_samples TAGS ts_tags TIME RANGES ts_time_ranges;

-- Series 201 ('bar') must not match the 'foo' selector, and it has a sample inside the requested
-- time range - so if the `id IN <tags subquery>` condition is ever lost from the generated query,
-- that sample leaks into the result and the test fails.
-- Series 103 and 104 intentionally have samples inside the requested interval but disjoint
-- time ranges, so the two bounds of the time-ranges filter are checked independently.
-- The table name is qualified because an asynchronous insert through a table function resolves an unqualified name in the default database.
INSERT INTO FUNCTION timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) (id, metric_name, tags) VALUES
    (101, 'foo', map('env', 'prod')),
    (102, 'foo', map('env', 'dev')),
    (103, 'foo', map('env', 'future')),
    (104, 'foo', map('env', 'past')),
    (201, 'bar', map());

INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts) (id, min_time, max_time) VALUES
    (101, toDateTime64(0, 3), toDateTime64(1000, 3)),
    (102, toDateTime64(0, 3), toDateTime64(1000, 3)),
    (103, toDateTime64(600, 3), toDateTime64(1000, 3)),
    (104, toDateTime64(0, 3), toDateTime64(50, 3)),
    (201, toDateTime64(0, 3), toDateTime64(1000, 3));

INSERT INTO ts_samples (id, timestamp, value) VALUES
    (101, toDateTime64(100, 3), 1.), (101, toDateTime64(200, 3), 2.), (101, toDateTime64(300, 3), 3.),
    (102, toDateTime64(150, 3), 10.), (102, toDateTime64(250, 3), 20.),
    (103, toDateTime64(200, 3), 30.), (104, toDateTime64(200, 3), 40.),
    (201, toDateTime64(100, 3), 100.), (201, toDateTime64(300, 3), 300.);

SELECT '-- timeSeriesSelector returns the same samples as a direct filtered read of the samples table';

SELECT id, timestamp, value
FROM timeSeriesSelector(ts, 'foo', 100, 250)
ORDER BY id, timestamp;

SELECT '(direct read for comparison)';

SELECT id, timestamp, value FROM ts_samples
WHERE (timestamp >= toDateTime64(100, 3)) AND (timestamp <= toDateTime64(250, 3)) AND (id IN (101, 102))
ORDER BY id, timestamp;

SELECT '-- the SELECT over the samples table uses bare columns, time range first';

-- Inspect only the final raw filter expression: separate action descriptions can mention the
-- timestamp before an incorrectly ordered conjunction. Ignore generated table qualifiers only.
SELECT count() = 1
           AND min(position(plan, 'greaterOrEquals(timestamp,')) > 0
           AND min(position(plan, 'greaterOrEquals(timestamp,')) < min(position(plan, 'lessOrEquals(timestamp,')) AS bare_lower_bound_before_upper_bound,
       count() = 1
           AND min(position(plan, 'lessOrEquals(timestamp,')) > 0
           AND min(position(plan, 'lessOrEquals(timestamp,')) < min(position(plan, 'in(id,')) AS bare_upper_bound_before_id_in
FROM
(
    SELECT replaceRegexpAll(explain, '__table[0-9]+\\.', '') AS plan
    FROM (EXPLAIN actions = 1, pretty = 0 SELECT id, timestamp, value FROM timeSeriesSelector(ts, 'foo', 100, 250) SETTINGS optimize_move_to_prewhere = 0)
)
WHERE startsWith(trimLeft(plan), 'Filter column:') AND position(plan, 'in(id,') > 0;

SELECT '-- a samples table whose physical type differs (here: by timezone only): bare conditions, cast in the outer SELECT';

-- The declared timestamp type of `ts` was captured as `DateTime64(3)` when the table was created,
-- so this ALTER changes only the physical type of the samples-table timestamp.
ALTER TABLE ts_samples
    MODIFY COLUMN timestamp DateTime64(3, 'UTC');

-- The types row of `TSVWithNamesAndTypes` shows the runtime type of the result.
SELECT id, timestamp, value FROM timeSeriesSelector(ts, 'foo', 100, 250) ORDER BY id, timestamp FORMAT TSVWithNamesAndTypes;

-- The cast of `timestamp` to the declared type appears only in the outer SELECT (the `Output:`
-- line of the plan), not in the conditions: comparing the bare columns is correct
-- because the timezone does not change the stored values.
SELECT '-- the bare timestamp condition and the timestamp cast in the outer SELECT';

SELECT position(filter, 'timestamp >=') > 0 AND position(filter, 'id IN') > 0
           AND position(filter, 'CAST(timestamp') = 0 AND position(filter, 'CAST(id') = 0 AS bare_timestamp_condition,
       position(plan, 'CAST(timestamp AS DateTime64(3))') > 0 AS timestamp_cast_in_outer_select
FROM
(
    SELECT arrayStringConcat(groupArray(explain), '\n') AS plan,
           arrayStringConcat(groupArrayIf(explain, position(explain, 'Filter column:') > 0), '\n') AS filter
    FROM (EXPLAIN actions = 1 SELECT id, timestamp, value FROM timeSeriesSelector(ts, 'foo', 100, 250) SETTINGS optimize_move_to_prewhere = 0)
);

DROP TABLE ts;
