-- Tags: no-fasttest, no-parallel-replicas
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-parallel-replicas: every parallel replica reads all rows of the table behind `timeSeriesSamples`, so each row
-- is returned once per replica until https://github.com/ClickHouse/ClickHouse/pull/119350 is merged.

-- `timeSeriesSelector` (and every PromQL selector evaluated through it) builds a SELECT over the
-- samples table. That inner SELECT must reference the bare `id` / `timestamp` / `value` columns
-- (no casts wrapping the columns in its SELECT list) and must put the selective timestamp range
-- condition before the `id IN <tags subquery>` condition. Wrapped primary-key columns are
-- re-evaluated over the whole primary index during index analysis, and they disable
-- primary-key-based selectivity estimation - then the PREWHERE read steps are misordered and
-- the expensive `in(id, set)` probe runs on all read rows. The casts to the declared types are
-- applied by an outer SELECT over the filtered subquery, so they run only on the rows which
-- passed the filter.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

-- The identifiers are written explicitly into the inner tables, so they are declared as UInt64.
CREATE TABLE ts ENGINE = TimeSeries TAGS INNER COLUMNS (id UInt64);

-- Series 201 ('bar') must not match the 'foo' selector, and it has a sample inside the requested
-- time range - so if the `id IN <tags subquery>` condition is ever lost from the generated query,
-- that sample leaks into the result and the test fails.
-- The table name is qualified because an asynchronous insert through a table function resolves an unqualified name in the default database.
INSERT INTO FUNCTION timeSeriesTags({CLICKHOUSE_DATABASE:Identifier}.ts) (id, metric_name, tags) VALUES
    (101, 'foo', map('env', 'prod')),
    (102, 'foo', map('env', 'dev')),
    (201, 'bar', map());

INSERT INTO FUNCTION timeSeriesTimeRanges({CLICKHOUSE_DATABASE:Identifier}.ts) (id, min_time, max_time) VALUES
    (101, toDateTime64(0, 3), toDateTime64(1000, 3)),
    (102, toDateTime64(0, 3), toDateTime64(1000, 3)),
    (201, toDateTime64(0, 3), toDateTime64(1000, 3));

INSERT INTO FUNCTION timeSeriesSamples({CLICKHOUSE_DATABASE:Identifier}.ts) (id, timestamp, value) VALUES
    (101, toDateTime64(100, 3), 1.), (101, toDateTime64(200, 3), 2.), (101, toDateTime64(300, 3), 3.),
    (102, toDateTime64(150, 3), 10.), (102, toDateTime64(250, 3), 20.),
    (201, toDateTime64(100, 3), 100.), (201, toDateTime64(300, 3), 300.);

SELECT '-- timeSeriesSelector returns the same rows as a direct filtered read of the samples table';

SELECT id, timestamp, value FROM timeSeriesSelector(ts, 'foo', 100, 250) ORDER BY id, timestamp;

SELECT '(direct read for comparison)';

SELECT id, timestamp, value FROM timeSeriesSamples(ts)
WHERE (timestamp >= toDateTime64(100, 3)) AND (timestamp <= toDateTime64(250, 3)) AND (id IN (101, 102))
ORDER BY id, timestamp;

SELECT '-- the SELECT over the samples table uses bare columns, timestamp range first';

-- The WHERE conditions must reference the bare `id` / `timestamp` columns in the generated order.
-- If aliased casts shadow the columns, the conditions render with the wrapped expressions
-- (e.g. `CAST(timestamp AS DateTime64(3)) >= ...`), and `position` returns 0 for both patterns.
-- `optimize_move_to_prewhere = 0` keeps the generated condition order.
SELECT position(plan, 'timestamp >=') BETWEEN 1 AND position(plan, 'id IN') AS bare_timestamp_condition_before_id_in
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, timestamp, value FROM timeSeriesSelector(ts, 'foo', 100, 250) SETTINGS optimize_move_to_prewhere = 0));

DROP TABLE ts;
