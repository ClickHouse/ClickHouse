-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables
-- synchronously, so the deferred inner DROPs are rejected with "ON CLUSTER is not allowed for Replicated database".

-- A `TimeSeries` id of type `Tuple(UInt64, LowCardinality(UUID))` keeps the identifiers
-- dictionary-encoded: the default id generator wraps the tags hash in `toLowCardinality`,
-- the whole-metric primary-key range conditions are still emitted, and `timeSeriesSelector`
-- reads and returns only the second id component (which alone identifies a time series under
-- the canonical generator), so the `IN <ids>` filter and `timeSeriesIdToGroup` work on
-- dictionaries instead of rows.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_lc;
DROP TABLE IF EXISTS ts_plain;
DROP TABLE IF EXISTS ts_custom_gen;

CREATE TABLE ts_lc ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, LowCardinality(UUID)));

SELECT '-- the default id generator dictionary-encodes the tags hash';

SELECT default_expression FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.tags.%' AND name = 'id';

INSERT INTO ts_lc (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.), (toDateTime64(300, 3), 3.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.), (toDateTime64(250, 3), 20.)]),
    ('foo', map(), [(toDateTime64(100, 3), 5.)]),
    ('bar', map('env', 'dev'), [(toDateTime64(100, 3), 100.), (toDateTime64(300, 3), 300.)]);

SELECT '-- the selector returns only the second id component, dictionary-encoded';

SELECT toTypeName(id) FROM timeSeriesSelector(ts_lc, 'foo', 0, 1000) LIMIT 1;

SELECT '-- whole-metric selector: results and the emitted id range';

SELECT timestamp, value FROM timeSeriesSelector(ts_lc, 'foo', 0, 1000) ORDER BY value, timestamp;

SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range, plan LIKE '%IN subquery%' AS keeps_id_set
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_lc, 'foo', 0, 1000)));

SELECT '-- partial selector (the id set is full-shaped for index analysis): results, no id range';

SELECT timestamp, value FROM timeSeriesSelector(ts_lc, 'foo{env="prod"}', 0, 1000) ORDER BY value, timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_lc, 'foo{env!=""}', 0, 1000) ORDER BY value, timestamp;

SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_lc, 'foo{env="prod"}', 0, 1000)));

SELECT '-- prometheus query evaluation over the narrowed ids';

SELECT arraySort(tags), round(value, 6) FROM prometheusQuery(ts_lc, 'foo', 250) ORDER BY ALL;
SELECT arraySort(tags), round(value, 6) FROM prometheusQuery(ts_lc, 'sum by (env) (rate(foo[5m]))', 400) ORDER BY ALL;

SELECT '-- the same queries over the plain layout return the same results';

CREATE TABLE ts_plain ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));

INSERT INTO ts_plain (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.), (toDateTime64(300, 3), 3.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.), (toDateTime64(250, 3), 20.)]),
    ('foo', map(), [(toDateTime64(100, 3), 5.)]),
    ('bar', map('env', 'dev'), [(toDateTime64(100, 3), 100.), (toDateTime64(300, 3), 300.)]);

SELECT arraySort(tags), round(value, 6) FROM prometheusQuery(ts_plain, 'foo', 250) ORDER BY ALL;
SELECT arraySort(tags), round(value, 6) FROM prometheusQuery(ts_plain, 'sum by (env) (rate(foo[5m]))', 400) ORDER BY ALL;

SELECT '-- a non-canonical generator: no narrowing, the full id is returned';

CREATE TABLE ts_custom_gen ENGINE = TimeSeries
TAGS INNER COLUMNS (id Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(tags), toLowCardinality(reinterpretAsUUID(sipHash128(metric_name, tags)))));

INSERT INTO ts_custom_gen (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]);

SELECT toTypeName(id) FROM timeSeriesSelector(ts_custom_gen, 'foo', 0, 1000) LIMIT 1;
SELECT timestamp, value FROM timeSeriesSelector(ts_custom_gen, 'foo', 0, 1000) ORDER BY value, timestamp;

DROP TABLE ts_lc;
DROP TABLE ts_plain;
DROP TABLE ts_custom_gen;
