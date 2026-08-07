-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: `DatabaseReplicated::dropTable` does not drop `TimeSeries` inner tables
-- synchronously, so the deferred inner DROPs are rejected with "ON CLUSTER is not allowed for Replicated database".

-- `timeSeriesSelector` (and every PromQL selector evaluated through it) builds a SELECT over the
-- samples table. When the samples table already stores exactly the requested column types, that
-- SELECT must reference the bare `id` / `timestamp` / `value` columns (no no-op `CAST(id, ...)` /
-- `toDateTime64(timestamp, ...)` / `toFloat64(value)` wrappers) and must put the selective
-- timestamp range condition before the `id IN <tags subquery>` condition. Wrapped primary-key
-- columns disable primary-key-based selectivity estimation (which misorders the PREWHERE read
-- steps and runs the expensive `in(id, set)` probe on all read rows), are re-evaluated over the
-- whole primary index during index analysis, and re-execute per row at scan time.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_tags;
DROP TABLE IF EXISTS ts_samples;
DROP TABLE IF EXISTS ts_samples_tz;
DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_cast;

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
    timestamp DateTime64(3),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts ENGINE = TimeSeries SAMPLES ts_samples TAGS ts_tags;

INSERT INTO ts_tags (id, metric_name, tags, min_time, max_time) VALUES
    (101, 'foo', map('env', 'prod'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (102, 'foo', map('env', 'dev'), toDateTime64(0, 3), toDateTime64(1000, 3)),
    (201, 'bar', map(), toDateTime64(0, 3), toDateTime64(1000, 3));

INSERT INTO ts_samples (id, timestamp, value) VALUES
    (101, toDateTime64(100, 3), 1.), (101, toDateTime64(200, 3), 2.), (101, toDateTime64(300, 3), 3.),
    (102, toDateTime64(150, 3), 10.), (102, toDateTime64(250, 3), 20.),
    (201, toDateTime64(100, 3), 100.), (201, toDateTime64(300, 3), 300.);

SELECT '-- timeSeriesSelector returns the same rows as a direct filtered read of the samples table';

SELECT id, timestamp, value FROM timeSeriesSelector(ts, 'foo', 100, 250) ORDER BY id, timestamp;

SELECT '(direct read for comparison)';

SELECT id, timestamp, value FROM ts_samples
WHERE (timestamp >= toDateTime64(100, 3)) AND (timestamp <= toDateTime64(250, 3)) AND (id IN (101, 102))
ORDER BY id, timestamp;

SELECT '-- selector with matchers';

SELECT id, timestamp, value FROM timeSeriesSelector(ts, 'foo{env="prod"}', 0, 1000) ORDER BY id, timestamp;
SELECT id, timestamp, value FROM timeSeriesSelector(ts, '{__name__=~"foo|bar", env!="dev"}', 0, 1000) ORDER BY id, timestamp;

SELECT '-- prometheusQuery evaluation over the same selector';

SELECT * FROM prometheusQuery(ts, 'foo', 250) ORDER BY ALL;
SELECT * FROM prometheusQueryRange(ts, 'sum by (env) (foo)', 100, 300, 100) ORDER BY ALL;

SELECT '-- the SELECT over the samples table uses bare columns, timestamp range first';

-- The plan of the query built over the samples table must not contain casts applied to the
-- `id` / `timestamp` / `value` columns (matches both `CAST(id, ...)` and `_CAST(id, ...)`).
SELECT plan NOT LIKE '%CAST(id%' AS id_is_bare,
       plan NOT LIKE '%toDateTime64(timestamp%' AS timestamp_is_bare,
       plan NOT LIKE '%toFloat64(value%' AS value_is_bare
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts, 'foo', 100, 250)));

-- The generated WHERE must list the timestamp range conditions before `id IN <subquery>`:
-- with `optimize_move_to_prewhere = 0` the filter keeps the generated condition order.
-- (The old shape rendered `CAST(id, ...) IN ...` and `toDateTime64(timestamp, ...) >= ...`,
-- which match neither pattern, so this correctly returns 0 for the old shape.)
SELECT position(plan, 'timestamp >=') BETWEEN 1 AND position(plan, 'id IN') AS timestamp_condition_before_id_in
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts, 'foo', 100, 250) SETTINGS optimize_move_to_prewhere = 0));

SELECT '-- a samples table whose physical type differs (here: by timezone only) still gets the cast, with the same results';

-- `DataTypeDateTime64::equals` ignores the timezone, so a samples table with an explicit timezone
-- is accepted by the TimeSeries engine while the requested timestamp type stays `DateTime64(3)`.
-- The cast must be kept in this case: eliding it would change the result header's type name.

CREATE TABLE ts_samples_tz
(
    id UInt64,
    timestamp DateTime64(3, 'UTC'),
    value Float64
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts_cast (`time_series` Array(Tuple(DateTime64(3), Float64))) ENGINE = TimeSeries SAMPLES ts_samples_tz TAGS ts_tags;

INSERT INTO ts_samples_tz SELECT id, timestamp, value FROM ts_samples;

SELECT id, timestamp, value, toTypeName(timestamp) FROM timeSeriesSelector(ts_cast, 'foo', 100, 250) ORDER BY id, timestamp;

SELECT plan LIKE '%toDateTime64(timestamp%' AS timestamp_is_cast,
       plan NOT LIKE '%CAST(id%' AS id_is_bare,
       plan NOT LIKE '%toFloat64(value%' AS value_is_bare
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT sum(value) FROM timeSeriesSelector(ts_cast, 'foo', 100, 250)));

DROP TABLE ts_cast;
DROP TABLE ts;
DROP TABLE ts_samples_tz;
DROP TABLE ts_samples;
DROP TABLE ts_tags;
