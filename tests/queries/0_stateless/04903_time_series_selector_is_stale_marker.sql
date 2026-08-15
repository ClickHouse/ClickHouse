-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- `timeSeriesSelector` declares `is_stale_marker` as its fourth column, so the SELECT it builds over
-- the samples table has to project that column as well: the requested columns are applied as an outer
-- `SELECT <requested columns> FROM (<generated query>)`, so a caller selecting or filtering the flag
-- would otherwise ask for a column the generated query never returns. PromQL evaluation reads the
-- column exactly that way (see fromSelector.cpp), but only indirectly through the translation to SQL -
-- this covers the table function's own contract.

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
    timestamp DateTime64(3),
    value Float64,
    is_stale_marker UInt8
) ENGINE = MergeTree() ORDER BY (id, timestamp);

CREATE TABLE ts ENGINE = TimeSeries SAMPLES ts_samples TAGS ts_tags;

INSERT INTO ts_tags (id, metric_name, tags, min_time, max_time) VALUES
    (101, 'foo', map(), toDateTime64(0, 3), toDateTime64(1000, 3));

-- A real sample, then a Prometheus stale marker (stored as a flagged row keeping the raw NaN in
-- `value`, see PrometheusRemoteWriteProtocol.cpp), then a real sample after the series recovered.
INSERT INTO ts_samples (id, timestamp, value, is_stale_marker) VALUES
    (101, toDateTime64(100, 3), 1., 0),
    (101, toDateTime64(200, 3), nan, 1),
    (101, toDateTime64(300, 3), 3., 0);

SELECT '-- is_stale_marker leaves the generated storage query';

SELECT id, timestamp, value, is_stale_marker FROM timeSeriesSelector(ts, 'foo', 100, 300) ORDER BY timestamp;

SELECT '-- the declared four columns and their types';

SELECT * FROM timeSeriesSelector(ts, 'foo', 100, 300) ORDER BY timestamp FORMAT TSVWithNamesAndTypes;

SELECT '-- filtering out the stale markers (what every _over_time function does)';

SELECT timestamp, value FROM timeSeriesSelector(ts, 'foo', 100, 300) WHERE NOT is_stale_marker ORDER BY timestamp;

SELECT '-- keeping only the stale markers';

SELECT timestamp, value FROM timeSeriesSelector(ts, 'foo', 100, 300) WHERE is_stale_marker ORDER BY timestamp;

SELECT '-- the column can be aggregated without being selected';

SELECT count(), sum(is_stale_marker) FROM timeSeriesSelector(ts, 'foo', 100, 300);

DROP TABLE ts;
DROP TABLE ts_samples;
DROP TABLE ts_tags;
