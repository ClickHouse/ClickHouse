-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables
-- synchronously, so the deferred inner DROPs are rejected with "ON CLUSTER is not allowed for Replicated database".

-- With `time_series_selector_relaxed_filtering = 1` a selector which matches ALL
-- time series of one metric reads the samples table without row-level filtering: every condition
-- goes under `indexHint()` and is used for primary-key granule pruning only. The read may then
-- return extra rows - samples outside the requested time range and samples of series which do not
-- belong to the selector. The SQL a PromQL query is transpiled to tolerates them: unknown
-- identifiers map to the UNKNOWN_GROUP sentinel of `timeSeriesIdToGroup` and are filtered out,
-- and out-of-range samples are ignored by the `timeSeries*ToGrid` aggregation windows (or dropped
-- by an explicit timestamp filter when the raw samples reach the result, e.g. for a matrix result).

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';
SET time_series_selector_relaxed_filtering = 1;

DROP TABLE IF EXISTS ts_relaxed;

CREATE TABLE ts_relaxed ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));

INSERT INTO ts_relaxed (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.), (toDateTime64(300, 3), 3.), (toDateTime64(400, 3), 2.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.), (toDateTime64(250, 3), 20.), (toDateTime64(350, 3), 15.)]),
    ('bar', map('env', 'dev'), [(toDateTime64(100, 3), 100.), (toDateTime64(300, 3), 300.)]);

SELECT '-- the generated WHERE carries everything under indexHint, no row-level filter';

SELECT plan LIKE '%indexHint%' AS has_index_hint, plan LIKE '%IN subquery%' AS keeps_id_set
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_relaxed, 'foo', 150, 250)));

SELECT '-- PromQL results are identical with relaxed and exact filtering';

SELECT * FROM prometheusQuery(ts_relaxed, 'sum(foo)', 250) ORDER BY ALL;
SELECT * FROM prometheusQuery(ts_relaxed, 'sum(foo)', 250) ORDER BY ALL SETTINGS time_series_selector_relaxed_filtering = 0;

SELECT * FROM prometheusQuery(ts_relaxed, 'sum by (env) (resets(foo[3m]))', 400) ORDER BY ALL;
SELECT * FROM prometheusQuery(ts_relaxed, 'sum by (env) (resets(foo[3m]))', 400) ORDER BY ALL SETTINGS time_series_selector_relaxed_filtering = 0;

SELECT '-- a matrix result gets an explicit timestamp filter: no out-of-range samples leak';

SELECT * FROM prometheusQuery(ts_relaxed, 'foo[2m]', 300) ORDER BY ALL;
SELECT * FROM prometheusQuery(ts_relaxed, 'foo[2m]', 300) ORDER BY ALL SETTINGS time_series_selector_relaxed_filtering = 0;

SELECT '-- unknown identifiers map to the sentinel group instead of an error';

SELECT timeSeriesIdToGroup(tuple(toUInt64(42), toUUID('11111111-2222-3333-4444-555555555555')));

SELECT '-- exact filtering on demand: a direct call returns exactly the matching samples';

SELECT value FROM timeSeriesSelector(ts_relaxed, 'foo', 150, 250) ORDER BY value
SETTINGS time_series_selector_relaxed_filtering = 0;

DROP TABLE ts_relaxed;
