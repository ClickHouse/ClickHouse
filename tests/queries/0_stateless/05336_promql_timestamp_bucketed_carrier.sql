-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
-- Tag no-replicated-database: the test drops the inner tables of `TimeSeries` synchronously.
-- The row (v7) and bucketed (v11) carriers must return sample timestamps for a selector,
-- and evaluation timestamps for a general expression, without hiding stale-marker or lookback behavior.

SET allow_experimental_time_series_table = 1;
SET enable_promql_native_plan = 0;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_timestamp_v7 SYNC;
DROP TABLE IF EXISTS ts_timestamp_v11 SYNC;
DROP TABLE IF EXISTS ts_timestamp_f32_v7 SYNC;
DROP TABLE IF EXISTS ts_timestamp_f32_v11 SYNC;

CREATE TABLE ts_timestamp_v7 (samples Array(Tuple(DateTime64(6, 'UTC'), Float64)))
ENGINE = TimeSeries SETTINGS version = 7, recent_samples_ttl_seconds = 0;
CREATE TABLE ts_timestamp_v11 (samples Array(Tuple(DateTime64(6, 'UTC'), Float64)))
ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = 0;

INSERT INTO ts_timestamp_v7 (metric_name, tags, samples) VALUES
    ('m', map('job', 'api'), [(toDateTime64('1970-01-01 00:01:00.500125', 6, 'UTC'), 1.0),
                            (toDateTime64('1970-01-01 00:02:00.250375', 6, 'UTC'), -2.0),
                            (toDateTime64('1970-01-01 00:03:00.750625', 6, 'UTC'), 3.0)]),
    ('stale', map('job', 'stale'), [(toDateTime64('1970-01-01 00:01:40.250375', 6, 'UTC'), 5.0),
                                  (toDateTime64('1970-01-01 00:02:30.250375', 6, 'UTC'), reinterpretAsFloat64(toUInt64(0x7ff0000000000002)))]),
    ('ordinary', map('job', 'ordinary'), [(toDateTime64('1970-01-01 00:01:30.500125', 6, 'UTC'), 5.0),
                                        (toDateTime64('1970-01-01 00:01:40.250375', 6, 'UTC'), reinterpretAsFloat64(toUInt64(0x7ff8000000000001)))]),
    ('grouped', map('job', 'a'), [(toDateTime64('1970-01-01 00:01:15.250125', 6, 'UTC'), 1.0)]),
    ('grouped', map('job', 'b'), [(toDateTime64('1970-01-01 00:02:45.500375', 6, 'UTC'), 2.0)]);

INSERT INTO ts_timestamp_v11 (metric_name, tags, samples)
SELECT metric_name, tags, samples FROM ts_timestamp_v7;

SELECT 'sample timestamp, fractional precision and dropped metric name';
SELECT 7 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:03:00.750625', 6, 'UTC'), 'Float64')],
    groupArray(timestamp) = [toDateTime64('1970-01-01 00:03:20.875', 6, 'UTC')],
    groupArray(tags) = [[('job', 'api')]], groupArray(toTypeName(value)) = ['Float64']
FROM prometheusQuery(ts_timestamp_v7, 'timestamp(m)', 200.875);
SELECT 11 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:03:00.750625', 6, 'UTC'), 'Float64')],
    groupArray(timestamp) = [toDateTime64('1970-01-01 00:03:20.875', 6, 'UTC')],
    groupArray(tags) = [[('job', 'api')]], groupArray(toTypeName(value)) = ['Float64']
FROM prometheusQuery(ts_timestamp_v11, 'timestamp(m)', 200.875);

SELECT 'offset changes selection, not the returned sample timestamp';
SELECT 7 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:02:00.250375', 6, 'UTC'), 'Float64')]
FROM prometheusQuery(ts_timestamp_v7, 'timestamp(m offset 1m)', 200.875);
SELECT 11 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:02:00.250375', 6, 'UTC'), 'Float64')]
FROM prometheusQuery(ts_timestamp_v11, 'timestamp(m offset 1m)', 200.875);

SELECT '@ selects a fixed older sample';
SELECT 7 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:01:00.500125', 6, 'UTC'), 'Float64')]
FROM prometheusQuery(ts_timestamp_v7, 'timestamp(m @ 100.875)', 200.875);
SELECT 11 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:01:00.500125', 6, 'UTC'), 'Float64')]
FROM prometheusQuery(ts_timestamp_v11, 'timestamp(m @ 100.875)', 200.875);

SELECT 'general expression returns the evaluation timestamp';
SELECT 7 AS version, count() = 1, groupArray(value) = [200.875]
FROM prometheusQuery(ts_timestamp_v7, 'timestamp(abs(m))', 200.875);
SELECT 11 AS version, count() = 1, groupArray(value) = [200.875]
FROM prometheusQuery(ts_timestamp_v11, 'timestamp(abs(m))', 200.875);

SELECT 'range query retains each sample timestamp';
SELECT 7 AS version, count() = 1,
    groupArray(arrayMap(x -> (toUnixTimestamp64Micro(x.1), x.2), samples))
        = [[(100875000, CAST(toDateTime64('1970-01-01 00:01:00.500125', 6, 'UTC'), 'Float64')),
            (150875000, CAST(toDateTime64('1970-01-01 00:02:00.250375', 6, 'UTC'), 'Float64')),
            (200875000, CAST(toDateTime64('1970-01-01 00:03:00.750625', 6, 'UTC'), 'Float64'))]]
FROM prometheusQueryRange(ts_timestamp_v7, 'timestamp(m)', 100.875, 200.875, 50);
SELECT 11 AS version, count() = 1,
    groupArray(arrayMap(x -> (toUnixTimestamp64Micro(x.1), x.2), samples))
        = [[(100875000, CAST(toDateTime64('1970-01-01 00:01:00.500125', 6, 'UTC'), 'Float64')),
            (150875000, CAST(toDateTime64('1970-01-01 00:02:00.250375', 6, 'UTC'), 'Float64')),
            (200875000, CAST(toDateTime64('1970-01-01 00:03:00.750625', 6, 'UTC'), 'Float64'))]]
FROM prometheusQueryRange(ts_timestamp_v11, 'timestamp(m)', 100.875, 200.875, 50);

SELECT 'series groups remain separate';
SELECT 7 AS version, count() = 2,
    arraySort(groupArray((tags, value)))
        = [([('job', 'a')], CAST(toDateTime64('1970-01-01 00:01:15.250125', 6, 'UTC'), 'Float64')),
           ([('job', 'b')], CAST(toDateTime64('1970-01-01 00:02:45.500375', 6, 'UTC'), 'Float64'))]
FROM prometheusQuery(ts_timestamp_v7, 'timestamp(grouped)', 200.875);
SELECT 11 AS version, count() = 2,
    arraySort(groupArray((tags, value)))
        = [([('job', 'a')], CAST(toDateTime64('1970-01-01 00:01:15.250125', 6, 'UTC'), 'Float64')),
           ([('job', 'b')], CAST(toDateTime64('1970-01-01 00:02:45.500375', 6, 'UTC'), 'Float64'))]
FROM prometheusQuery(ts_timestamp_v11, 'timestamp(grouped)', 200.875);

SELECT 'sample before the stale marker';
SELECT 7 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:01:40.250375', 6, 'UTC'), 'Float64')]
FROM prometheusQuery(ts_timestamp_v7, 'timestamp(stale)', 140.875);
SELECT 11 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:01:40.250375', 6, 'UTC'), 'Float64')]
FROM prometheusQuery(ts_timestamp_v11, 'timestamp(stale)', 140.875);

SELECT 'a stale marker hides the series instead of reviving the preceding sample';
SELECT 7 AS version, count() = 0 FROM prometheusQuery(ts_timestamp_v7, 'timestamp(stale)', 200.875);
SELECT 11 AS version, count() = 0 FROM prometheusQuery(ts_timestamp_v11, 'timestamp(stale)', 200.875);

SELECT 'range query omits the stale steps';
SELECT 7 AS version, count() = 1,
    groupArray(arrayMap(x -> (toUnixTimestamp64Micro(x.1), x.2), samples))
        = [[(100875000, CAST(toDateTime64('1970-01-01 00:01:40.250375', 6, 'UTC'), 'Float64'))]]
FROM prometheusQueryRange(ts_timestamp_v7, 'timestamp(stale)', 100.875, 200.875, 50);
SELECT 11 AS version, count() = 1,
    groupArray(arrayMap(x -> (toUnixTimestamp64Micro(x.1), x.2), samples))
        = [[(100875000, CAST(toDateTime64('1970-01-01 00:01:40.250375', 6, 'UTC'), 'Float64'))]]
FROM prometheusQueryRange(ts_timestamp_v11, 'timestamp(stale)', 100.875, 200.875, 50);

SELECT 'an ordinary NaN is not a stale marker';
SELECT 7 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:01:40.250375', 6, 'UTC'), 'Float64')]
FROM prometheusQuery(ts_timestamp_v7, 'timestamp(ordinary)', 140.875);
SELECT 11 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:01:40.250375', 6, 'UTC'), 'Float64')]
FROM prometheusQuery(ts_timestamp_v11, 'timestamp(ordinary)', 140.875);

SELECT 'one tick inside the lookback boundary';
SELECT 7 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:03:00.750625', 6, 'UTC'), 'Float64')]
FROM prometheusQuery(ts_timestamp_v7, 'timestamp(m)', toDateTime64('1970-01-01 00:08:00.750624', 6, 'UTC'));
SELECT 11 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('1970-01-01 00:03:00.750625', 6, 'UTC'), 'Float64')]
FROM prometheusQuery(ts_timestamp_v11, 'timestamp(m)', toDateTime64('1970-01-01 00:08:00.750624', 6, 'UTC'));

SELECT 'the left lookback boundary is excluded';
SELECT 7 AS version, count() = 0
FROM prometheusQuery(ts_timestamp_v7, 'timestamp(m)', toDateTime64('1970-01-01 00:08:00.750625', 6, 'UTC'));
SELECT 11 AS version, count() = 0
FROM prometheusQuery(ts_timestamp_v11, 'timestamp(m)', toDateTime64('1970-01-01 00:08:00.750625', 6, 'UTC'));

SELECT 'an empty selector remains empty';
SELECT 7 AS version, count() = 0 FROM prometheusQuery(ts_timestamp_v7, 'timestamp(missing)', 200.875);
SELECT 11 AS version, count() = 0 FROM prometheusQuery(ts_timestamp_v11, 'timestamp(missing)', 200.875);

SELECT 'Float32 samples do not narrow the timestamp value';
CREATE TABLE ts_timestamp_f32_v7 (samples Array(Tuple(DateTime64(3, 'UTC'), Float32)))
ENGINE = TimeSeries SETTINGS version = 7, recent_samples_ttl_seconds = 0;
CREATE TABLE ts_timestamp_f32_v11 (samples Array(Tuple(DateTime64(3, 'UTC'), Float32)))
ENGINE = TimeSeries SETTINGS version = 11, recent_samples_ttl_seconds = 0;
INSERT INTO ts_timestamp_f32_v7 (metric_name, tags, samples) VALUES
    ('up', map('job', 'float32'), [(toDateTime64('2025-11-30 08:30:05.125', 3, 'UTC'), 1)]);
INSERT INTO ts_timestamp_f32_v11 (metric_name, tags, samples)
SELECT metric_name, tags, samples FROM ts_timestamp_f32_v7;
SELECT 7 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('2025-11-30 08:30:05.125', 3, 'UTC'), 'Float64')],
    groupArray(toTypeName(value)) = ['Float64']
FROM prometheusQuery(ts_timestamp_f32_v7, 'timestamp(up)', toDateTime64('2025-11-30 08:30:10', 3, 'UTC'));
SELECT 11 AS version, count() = 1,
    groupArray(value) = [CAST(toDateTime64('2025-11-30 08:30:05.125', 3, 'UTC'), 'Float64')],
    groupArray(toTypeName(value)) = ['Float64']
FROM prometheusQuery(ts_timestamp_f32_v11, 'timestamp(up)', toDateTime64('2025-11-30 08:30:10', 3, 'UTC'));

-- Invalid argument types must still fail instead of returning an empty or evaluation-timestamp result.
SELECT * FROM prometheusQuery(ts_timestamp_v7, 'timestamp(1)', 200.875); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_timestamp_v11, 'timestamp(1)', 200.875); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_timestamp_v7, 'timestamp(m[1m])', 200.875); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery(ts_timestamp_v11, 'timestamp(m[1m])', 200.875); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE ts_timestamp_v7 SYNC;
DROP TABLE ts_timestamp_v11 SYNC;
DROP TABLE ts_timestamp_f32_v7 SYNC;
DROP TABLE ts_timestamp_f32_v11 SYNC;
