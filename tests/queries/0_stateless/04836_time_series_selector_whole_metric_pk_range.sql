-- Tags: no-fasttest, long
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `timeSeriesSelector` (and every PromQL selector evaluated through it) filters the samples table with
-- `id IN <tags subquery>`. With a two-component id layout `Tuple(hash(metric_name), hash(tags))`
-- all series of one metric occupy one continuous primary-key range. When a selector matches the WHOLE
-- metric (verified by a probe on the tags table at query build time), the generated WHERE additionally
-- carries `id >= tuple(hash('metric'), min) AND id <= tuple(hash('metric'), max)` and the id set is
-- excluded from primary-key index analysis (`use_index_for_in_with_subqueries_max_values = 1`): index
-- analysis then works on the continuous range instead of running a generic exclusion search with the
-- whole set. The `id IN <set>` condition always stays in the WHERE, so the returned rows never change.
-- The range conditions contain the max-UUID literal, which is used below to detect the emission.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_clustered;
DROP TABLE IF EXISTS ts_plain;
DROP TABLE IF EXISTS ts_custom_gen;
DROP TABLE IF EXISTS ts_altered_gen;
DROP TABLE IF EXISTS ts_u64;

-- The metric-clustered layout: id = tuple(sipHash64(metric_name), reinterpretAsUUID(sipHash128(tags))).
-- Inner target tables (their names contain dots and the table UUID - the qualified references of the
-- range conditions must still resolve).
CREATE TABLE ts_clustered ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UUID));

INSERT INTO ts_clustered (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.), (toDateTime64(300, 3), 3.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.), (toDateTime64(250, 3), 20.)]),
    ('foo', map(), [(toDateTime64(100, 3), 5.)]),
    ('bar', map('env', 'dev'), [(toDateTime64(100, 3), 100.), (toDateTime64(300, 3), 300.)]);

SELECT '-- whole-metric selector: same rows as a filtered read, and the WHERE carries the id range';

SELECT timestamp, value FROM timeSeriesSelector(ts_clustered, 'foo', 0, 1000) ORDER BY value, timestamp;

SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range, plan LIKE '%IN subquery%' AS keeps_id_set
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_clustered, 'foo', 0, 1000)));

SELECT '-- whole-metric-by-data selector (a matcher every series passes): the range is still emitted';

SELECT timestamp, value FROM timeSeriesSelector(ts_clustered, 'foo{env=~".*"}', 0, 1000) ORDER BY value, timestamp;

SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_clustered, 'foo{env=~".*"}', 0, 1000)));

SELECT '-- partial-metric selector (a matcher filters some series out): falls back to the id set only';

SELECT timestamp, value FROM timeSeriesSelector(ts_clustered, 'foo{env="prod"}', 0, 1000) ORDER BY value, timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_clustered, 'foo{env!=""}', 0, 1000) ORDER BY value, timestamp;

SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_clustered, 'foo{env="prod"}', 0, 1000)));

SELECT '-- regex matcher on the metric name: falls back';

SELECT timestamp, value FROM timeSeriesSelector(ts_clustered, '{__name__=~"foo|bar", env="dev"}', 0, 1000) ORDER BY value, timestamp;

SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_clustered, '{__name__=~"foo|bar"}', 0, 1000)));

SELECT '-- selector with no series in the time range: empty result either way';

SELECT count() FROM timeSeriesSelector(ts_clustered, 'foo', 2000, 3000);
SELECT count() FROM timeSeriesSelector(ts_clustered, 'no_such_metric', 0, 1000);

SELECT '-- full PromQL evaluations over whole-metric and partial selectors';

SELECT * FROM prometheusQuery(ts_clustered, 'sum(foo)', 250) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts_clustered, 'sum by (env) (foo)', 100, 300, 100) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts_clustered, 'foo{env="dev"} or bar', 100, 300, 100) ORDER BY ALL;

SELECT '-- the id set stays in the row-level filter: series of another metric are never returned';

-- `foo` and `bar` have different first id components, so this holds by the range too; the check
-- documents that the kept `id IN <set>` condition is what guarantees it on any layout.
SELECT count() FROM timeSeriesSelector(ts_clustered, 'foo', 0, 1000)
WHERE id IN (SELECT id FROM timeSeriesSelector(ts_clustered, 'bar', 0, 1000));

SELECT '-- single-component id layout: no metric clustering, no id range, same results';

CREATE TABLE ts_plain ENGINE = TimeSeries TAGS INNER COLUMNS (id UUID);

INSERT INTO ts_plain (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.), (toDateTime64(200, 3), 2.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]);

SELECT timestamp, value FROM timeSeriesSelector(ts_plain, 'foo', 0, 1000) ORDER BY value, timestamp;

SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_plain, 'foo', 0, 1000)));

SELECT '-- custom id generator: no structural guarantee, no id range, same results';

CREATE TABLE ts_custom_gen ENGINE = TimeSeries
TAGS INNER COLUMNS (id Tuple(UInt64, UUID) DEFAULT tuple(sipHash64(tags), reinterpretAsUUID(sipHash128(metric_name, tags))));

INSERT INTO ts_custom_gen (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]);

SELECT timestamp, value FROM timeSeriesSelector(ts_custom_gen, 'foo', 0, 1000) ORDER BY value, timestamp;

SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_custom_gen, 'foo', 0, 1000)));

SELECT '-- id_generator changed after ingestion: old series ids are outside the range, the probe detects them and falls back';

CREATE TABLE ts_altered_gen ENGINE = TimeSeries
TAGS INNER COLUMNS (id Tuple(UInt64, UUID) DEFAULT tuple(sipHash64(tags), reinterpretAsUUID(sipHash128(metric_name, tags))));

INSERT INTO ts_altered_gen (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'old'), [(toDateTime64(100, 3), 1.)]);

-- The setting overrides the column DEFAULT, so new inserts get canonical (metric-clustered) ids.
ALTER TABLE ts_altered_gen MODIFY SETTING id_generator = 'tuple(sipHash64(metric_name), reinterpretAsUUID(sipHash128(tags)))';

INSERT INTO ts_altered_gen (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'new'), [(toDateTime64(200, 3), 2.)]);

-- Both the old-generator and the new-generator series must be returned.
SELECT timestamp, value FROM timeSeriesSelector(ts_altered_gen, 'foo', 0, 1000) ORDER BY value, timestamp;

SELECT plan LIKE '%ffffffff-ffff-ffff-ffff-ffffffffffff%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_altered_gen, 'foo', 0, 1000)));

SELECT '-- Tuple(UInt64, UInt64) id layout: the range is emitted with the max-UInt64 literal';

CREATE TABLE ts_u64 ENGINE = TimeSeries TAGS INNER COLUMNS (id Tuple(UInt64, UInt64));

INSERT INTO ts_u64 (metric_name, tags, time_series) VALUES
    ('foo', map('env', 'prod'), [(toDateTime64(100, 3), 1.)]),
    ('foo', map('env', 'dev'), [(toDateTime64(150, 3), 10.)]);

SELECT timestamp, value FROM timeSeriesSelector(ts_u64, 'foo', 0, 1000) ORDER BY value, timestamp;

SELECT plan LIKE '%18446744073709551615%' AS has_id_range
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_u64, 'foo', 0, 1000)));

DROP TABLE ts_u64;
DROP TABLE ts_altered_gen;
DROP TABLE ts_custom_gen;
DROP TABLE ts_plain;
DROP TABLE ts_clustered;
