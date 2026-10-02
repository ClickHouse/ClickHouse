-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables
-- synchronously, so the deferred inner DROPs are rejected with "ON CLUSTER is not allowed for Replicated database".

-- A `TimeSeries` id of type `Tuple(UInt64, LowCardinality(UUID))` keeps the identifiers
-- dictionary-encoded: the default id generator wraps the tags hash in `toLowCardinality`,
-- and the range of the selected ids still replaces the id set in primary-key index analysis
-- of a whole-metric selector. Queries behave exactly as with a plain `Tuple(UInt64, UUID)` id.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_lc;
DROP TABLE IF EXISTS ts_plain;
DROP TABLE IF EXISTS ts_custom_gen;

-- The dictionary-encoded layout is the default id type now.
CREATE TABLE ts_lc ENGINE = TimeSeries;

SELECT '-- the default id type is dictionary-encoded';

SELECT type FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.tags.%' AND name = 'id';

SELECT '-- the default id generator dictionary-encodes the tags hash';

SELECT default_expression FROM system.columns
WHERE database = currentDatabase() AND table LIKE '.inner_id.tags.%' AND name = 'id';

INSERT INTO ts_lc (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.), (toDateTime64(300, 3), 3.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.), (toDateTime64(250, 3), 20.)]),
    ('foo', map(), [(toDateTime64(100, 3), 5.)]),
    ('bar', map('env', 'dev'), [(toDateTime64(100, 3), 100.), (toDateTime64(300, 3), 300.)]);

SELECT '-- the selector returns the id with the declared type';

SELECT toTypeName(id) FROM timeSeriesSelector(ts_lc, 'foo', 0, 1000) LIMIT 1;

SELECT '-- whole-metric selector: results and the emitted id range';

SELECT timestamp, value FROM timeSeriesSelector(ts_lc, 'foo', 0, 1000) ORDER BY value, timestamp;

SELECT
    countIf(explain ILIKE '%Condition:%' AND explain LIKE '%(id in [%') > 0 AS has_id_range,
    countIf(explain ILIKE '%Condition:%' AND explain ILIKE '%-element set%') > 0 AS id_set_in_index_analysis
FROM (EXPLAIN indexes = 1 SELECT sum(value) FROM timeSeriesSelector(ts_lc, 'foo', 0, 1000));

SELECT '-- partial selector: results and the id set alone';

SELECT timestamp, value FROM timeSeriesSelector(ts_lc, 'foo{env="prod"}', 0, 1000) ORDER BY value, timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_lc, 'foo{env!=""}', 0, 1000) ORDER BY value, timestamp;

SELECT
    countIf(explain ILIKE '%Condition:%' AND explain LIKE '%(id in [%') > 0 AS has_id_range,
    countIf(explain ILIKE '%Condition:%' AND explain ILIKE '%-element set%') > 0 AS id_set_in_index_analysis
FROM (EXPLAIN indexes = 1 SELECT sum(value) FROM timeSeriesSelector(ts_lc, 'foo{env="prod"}', 0, 1000));

SELECT '-- prometheus query evaluation over the dictionary-encoded ids';

SELECT arraySort(tags), round(value, 6) FROM prometheusQuery(ts_lc, 'foo', 250) ORDER BY ALL;
SELECT arraySort(tags), round(value, 6) FROM prometheusQuery(ts_lc, 'sum by (env) (rate(foo[5m]))', 400) ORDER BY ALL;

SELECT '-- the same queries over the plain layout return the same results';

CREATE TABLE ts_plain ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));

INSERT INTO ts_plain (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.), (toDateTime64(300, 3), 3.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.), (toDateTime64(250, 3), 20.)]),
    ('foo', map(), [(toDateTime64(100, 3), 5.)]),
    ('bar', map('env', 'dev'), [(toDateTime64(100, 3), 100.), (toDateTime64(300, 3), 300.)]);

SELECT arraySort(tags), round(value, 6) FROM prometheusQuery(ts_plain, 'foo', 250) ORDER BY ALL;
SELECT arraySort(tags), round(value, 6) FROM prometheusQuery(ts_plain, 'sum by (env) (rate(foo[5m]))', 400) ORDER BY ALL;

SELECT '-- a custom generator with a LowCardinality id also works';

CREATE TABLE ts_custom_gen ENGINE = TimeSeries
TAGS INNER COLUMNS (id Tuple(UInt64, LowCardinality(UUID)) DEFAULT tuple(sipHash64(tags), toLowCardinality(reinterpretAsUUID(sipHash128(metric_name, tags)))));

INSERT INTO ts_custom_gen (metric_name, tags, samples) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]);

SELECT toTypeName(id) FROM timeSeriesSelector(ts_custom_gen, 'foo', 0, 1000) LIMIT 1;
SELECT timestamp, value FROM timeSeriesSelector(ts_custom_gen, 'foo', 0, 1000) ORDER BY value, timestamp;

DROP TABLE ts_lc;
DROP TABLE ts_plain;
DROP TABLE ts_custom_gen;
