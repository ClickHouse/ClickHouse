-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Since version 2 of the TimeSeries table engine a row of the samples table contains the samples of one series
-- within one time bucket: the columns `samples` (a sorted array of tuples (timestamp, value)), `bucket`, `min_time`, `max_time`.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;

-- The recent samples table is disabled here because its TTL would drop the old samples inserted below.
-- Background merges of the samples table are disabled (`max_bytes_to_merge_at_max_space_in_pool = 1`) because the test below
-- checks the unmerged rows; `OPTIMIZE TABLE ... FINAL` still merges them.
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 600, recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

SELECT '-- the inserted samples are sorted, deduplicated (the greatest value wins, NaN loses) and split into buckets';

INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('m', map('env', 'prod'), [(toDateTime64(1250, 3), 5.), (toDateTime64(100, 3), 1.), (toDateTime64(700, 3), 3.), (toDateTime64(100, 3), 2.), (toDateTime64(1199.999, 3), 4.), (toDateTime64(700, 3), nan)]),
    ('m', map('env', 'dev'), [(toDateTime64(50, 3), 10.)]);

SELECT t.tags['env'] AS env, s.bucket, s.samples, s.min_time, s.max_time
FROM timeSeriesSamples(ts) AS s JOIN timeSeriesTags(ts) AS t ON s.id = t.id
ORDER BY env, s.bucket;

SELECT '-- reading the TimeSeries table returns a row per bucket, the rows of a series can be merged with timeSeriesGroupArray';

SELECT metric_name, tags, time_series FROM ts ORDER BY tags, time_series;
SELECT metric_name, tags, timeSeriesGroupArray(time_series) FROM ts FINAL GROUP BY metric_name, tags ORDER BY tags;

SELECT '-- the rows of the same bucket are merged by the engine';

INSERT INTO ts (metric_name, tags, time_series) VALUES ('m', map('env', 'prod'), [(toDateTime64(150, 3), 1.5), (toDateTime64(100, 3), 1.)]);
SELECT count() FROM timeSeriesSamples(ts);
SELECT '-- without FINAL the rows of the unmerged parts are returned as they are, with FINAL the rows of the same bucket are merged';
SELECT metric_name, tags, time_series FROM ts ORDER BY tags, time_series;
SELECT metric_name, tags, time_series FROM ts FINAL ORDER BY tags, time_series;
SELECT '-- timeSeriesSelector and PromQL merge the rows of the unmerged parts too (the greatest value wins for the duplicate timestamp 100)';
SELECT timeSeriesGroupArray(time_series) FROM timeSeriesSelector(ts, 'm{env="prod"}', 0, 200) GROUP BY id;
SELECT * FROM prometheusQuery(ts, 'm{env="prod"}[5m]', 200);

OPTIMIZE TABLE ts FINAL;
SELECT count() FROM timeSeriesSamples(ts);
SELECT t.tags['env'] AS env, s.bucket, s.samples, s.min_time, s.max_time
FROM timeSeriesSamples(ts) AS s JOIN timeSeriesTags(ts) AS t ON s.id = t.id
WHERE s.bucket = toDateTime64(0, 3)
ORDER BY env;

SELECT '-- timeSeriesSelector returns the buckets cut to the requested interval, the rows of a series can be merged with timeSeriesGroupArray';

DESCRIBE timeSeriesSelector(ts, 'm', 0, 1);
SELECT time_series FROM timeSeriesSelector(ts, 'm{env="prod"}', 120, 1200) ORDER BY time_series;
SELECT timeSeriesGroupArray(time_series) FROM timeSeriesSelector(ts, 'm{env="prod"}', 120, 1200) GROUP BY id;
SELECT count() FROM timeSeriesSelector(ts, 'm{env="prod"}', 200, 600);
SELECT count() FROM timeSeriesSelector(ts, 'm{env="prod"}', 1300, 2000);
-- The bucket 600 has samples at 700 and 1199.999, so its row matches the interval [800, 1000] by `min_time` and `max_time`
-- but has no samples in it; such rows are filtered out.
SELECT count() FROM timeSeriesSelector(ts, 'm{env="prod"}', 800, 1000);

-- The generated query filters the samples table by the `bucket`, `min_time` and `max_time` columns.
SELECT plan LIKE '%bucket%' AS filters_by_bucket, plan LIKE '%max_time%' AS filters_by_max_time, plan LIKE '%min_time%' AS filters_by_min_time,
       plan LIKE '%timeSeriesSliceSortedArray%' AS slices_samples
FROM (SELECT arrayStringConcat(groupArray(explain), '\n') AS plan FROM (EXPLAIN actions = 1 SELECT id, time_series FROM timeSeriesSelector(ts, 'm', 120, 1200)));

DROP TABLE ts;

SELECT '-- the recent samples table has its own bucket step';

DROP TABLE IF EXISTS ts_recent;
CREATE TABLE ts_recent ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 864000, recent_samples_bucket_step_seconds = 300;

-- 12 samples in the last 20 minutes.
INSERT INTO ts_recent (metric_name, tags, time_series) SELECT 'm', map(), arrayMap(i -> (toDateTime64(now(), 3) - toIntervalSecond(60 + i * 100), toFloat64(i)), range(12));

-- Every sample of a row belongs to its bucket, and `min_time` and `max_time` are the bounds of the row.
SELECT sum(length(samples)), count() BETWEEN 1 AND 2, countIf(toUInt32(bucket) % 3600 != 0),
       countIf(arrayExists(x -> (x.1 < bucket) OR (x.1 >= bucket + INTERVAL 3600 SECOND), samples)),
       countIf((min_time != samples[1].1) OR (max_time != samples[-1].1))
FROM timeSeriesSamples(ts_recent);
SELECT sum(length(samples)), count() BETWEEN 4 AND 5, countIf(toUInt32(bucket) % 300 != 0),
       countIf(arrayExists(x -> (x.1 < bucket) OR (x.1 >= bucket + INTERVAL 300 SECOND), samples)),
       countIf((min_time != samples[1].1) OR (max_time != samples[-1].1))
FROM merge(currentDatabase(), '^\\.inner_id\\.recentsamples\\.');

DROP TABLE ts_recent;

SELECT '-- the bucket steps are validated and cannot be altered';

CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS samples_bucket_step_seconds = 0; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_bucket_step_seconds = 0; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, recent_samples_bucket_step_seconds = 60; -- { serverError INVALID_SETTING_VALUE }
CREATE TABLE ts_bad ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
ALTER TABLE ts_bad MODIFY SETTING samples_bucket_step_seconds = 60; -- { serverError NOT_IMPLEMENTED }
DROP TABLE ts_bad;

SELECT '-- the columns of the older layout are rejected';

CREATE TABLE ts_bad ENGINE = TimeSeries SAMPLES INNER COLUMNS (timestamp DateTime64(6)); -- { serverError INCORRECT_QUERY }
CREATE TABLE ts_bad ENGINE = TimeSeries SAMPLES INNER COLUMNS (value Float32); -- { serverError INCORRECT_QUERY }

SELECT '-- the types of timestamps and values can be adjusted via the outer column or via the samples column';

DROP TABLE IF EXISTS ts_types;
CREATE TABLE ts_types (time_series Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_types';
INSERT INTO ts_types (metric_name, tags, time_series) VALUES ('m', map(), [(100, 1.5), (50, 2.5)]);
SELECT time_series FROM ts_types;
SELECT bucket, samples, min_time, max_time FROM timeSeriesSamples(ts_types);
SELECT * FROM timeSeriesSelector(ts_types, 'm', 60, 100) FORMAT TSVWithNamesAndTypes;
DROP TABLE ts_types;

CREATE TABLE ts_types ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (samples SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(6), value Float32))) CODEC(ZSTD(1)));
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 'ts_types' AND name = 'time_series';
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE') FROM system.tables WHERE database = currentDatabase() AND name = 'ts_types';
INSERT INTO ts_types (metric_name, tags, time_series) VALUES ('m', map(), [(toDateTime64(100.000001, 6), 1.5)]);
SELECT time_series FROM ts_types;
DROP TABLE ts_types;

SELECT '-- an external samples table can use plain types';

DROP TABLE IF EXISTS ts_ext;
DROP TABLE IF EXISTS ext_samples;
CREATE TABLE ext_samples (id UUID, samples Array(Tuple(DateTime64(3), Float64)), bucket DateTime64(3), min_time DateTime64(3), max_time DateTime64(3))
ENGINE = MergeTree ORDER BY (id, bucket);
CREATE TABLE ts_ext ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0 SAMPLES ext_samples TAGS INNER COLUMNS (id UUID);
INSERT INTO ts_ext (metric_name, tags, time_series) VALUES ('m', map(), [(toDateTime64(4000, 3), 1.), (toDateTime64(100, 3), 2.)]);
SELECT bucket, samples, min_time, max_time FROM ext_samples ORDER BY bucket;
SELECT * FROM prometheusQuery(ts_ext, 'm', 4100) ORDER BY ALL;
DROP TABLE ts_ext;
DROP TABLE ext_samples;

SELECT '-- an external samples table of a wrong layout is rejected';

DROP TABLE IF EXISTS ext_bad;
CREATE TABLE ext_bad (id UUID, timestamp DateTime64(3), value Float64) ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE ts_bad ENGINE = TimeSeries SAMPLES ext_bad TAGS INNER COLUMNS (id UUID); -- { serverError INCORRECT_QUERY }
DROP TABLE ext_bad;
CREATE TABLE ext_bad (id UUID, samples Array(Tuple(DateTime64(3), Float64)), bucket DateTime64(3)) ENGINE = MergeTree ORDER BY (id, bucket);
CREATE TABLE ts_bad ENGINE = TimeSeries SAMPLES ext_bad TAGS INNER COLUMNS (id UUID); -- { serverError THERE_IS_NO_COLUMN }
DROP TABLE ext_bad;
CREATE TABLE ext_bad (id UUID, samples Array(Tuple(DateTime64(3), Float64)), bucket String, min_time DateTime64(3), max_time DateTime64(3)) ENGINE = MergeTree ORDER BY (id, bucket);
CREATE TABLE ts_bad ENGINE = TimeSeries SAMPLES ext_bad TAGS INNER COLUMNS (id UUID); -- { serverError BAD_TYPE_OF_FIELD }
DROP TABLE ext_bad;
CREATE TABLE ext_bad (id UUID, samples Array(Tuple(DateTime64(3), Float32)), bucket DateTime64(3), min_time DateTime64(3), max_time DateTime64(3)) ENGINE = MergeTree ORDER BY (id, bucket);
CREATE TABLE ts_bad (time_series Array(Tuple(DateTime64(3), Float64))) ENGINE = TimeSeries SAMPLES ext_bad TAGS INNER COLUMNS (id UUID); -- { serverError BAD_TYPE_OF_FIELD }
DROP TABLE ext_bad;

SELECT '-- a table created AS a table of an older version gets the new layout with the same types';

DROP TABLE IF EXISTS ts_v1;
DROP TABLE IF EXISTS ts_v1_copy;
CREATE TABLE ts_v1 ENGINE = TimeSeries SETTINGS version = 1, recent_samples_ttl_seconds = 0
SAMPLES INNER COLUMNS (timestamp DateTime64(6) CODEC(Delta, ZSTD), value Float32);
SELECT extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE'), extract(create_table_query, 'SAMPLES INNER ENGINE = (.*?) TAGS INNER')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v1';
CREATE TABLE ts_v1_copy AS ts_v1;
SELECT extract(create_table_query, 'version = (\d+)'), extract(create_table_query, 'SAMPLES INNER COLUMNS \((.*?)\) SAMPLES INNER ENGINE'), extract(create_table_query, 'SAMPLES INNER ENGINE = (.*?) TAGS INNER')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_v1_copy';
DROP TABLE ts_v1_copy;
DROP TABLE ts_v1;
