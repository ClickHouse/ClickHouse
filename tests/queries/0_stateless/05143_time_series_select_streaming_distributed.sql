-- Tags: no-fasttest, distributed
-- Tag no-fasttest: the `TimeSeries` engine is disabled in the fast-test build.

SET allow_experimental_time_series_table = 1;
SET max_threads = 1;
SET max_block_size = 8192;
SET prefer_localhost_replica = 0;
SET analyzer_inline_views = 1;
SET optimize_trivial_view_pushdown_to_distributed = 1;

DROP TABLE IF EXISTS ts_stream_roundtrip;
DROP TABLE IF EXISTS ts_stream_remote;
DROP TABLE IF EXISTS ts_stream_dist;
DROP TABLE IF EXISTS ts_stream_remote_data;
CREATE TABLE ts_stream_remote_data (id UInt64, timestamp DateTime64(3), value Float64)
ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO ts_stream_remote_data SELECT number % 2, fromUnixTimestamp64Milli(number), number FROM numbers(131072);

SELECT '-- one shard';
CREATE TABLE ts_stream_dist AS ts_stream_remote_data
ENGINE = Distributed(test_shard_localhost, currentDatabase(), ts_stream_remote_data);
CREATE TABLE ts_stream_remote ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES ts_stream_dist TAGS INNER COLUMNS (id UInt64);

-- The shard must return raw blocks, so local block assembly can stop before reading every sample.
SELECT min(length(time_series) BETWEEN 1 AND 8192), count()
FROM (SELECT time_series FROM ts_stream_remote LIMIT 5);
SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series))) FROM ts_stream_remote;
SELECT arraySort(groupArray(length(time_series))) FROM ts_stream_remote FINAL;

-- Reading a `TimeSeries` table remotely must also work when plans are sent over the network.
CREATE TABLE ts_stream_roundtrip AS ts_stream_remote
ENGINE = Distributed(test_shard_localhost, currentDatabase(), ts_stream_remote);
SELECT min(length(time_series) BETWEEN 1 AND 8192), count()
FROM (SELECT time_series FROM ts_stream_roundtrip LIMIT 5) SETTINGS serialize_query_plan = 1;
SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series)))
FROM ts_stream_roundtrip SETTINGS serialize_query_plan = 1;
DROP TABLE ts_stream_roundtrip;

DROP TABLE ts_stream_remote;
DROP TABLE ts_stream_dist;

SELECT '-- two shards';
CREATE TABLE ts_stream_dist AS ts_stream_remote_data
ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), ts_stream_remote_data);
CREATE TABLE ts_stream_remote ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES ts_stream_dist TAGS INNER COLUMNS (id UInt64);

-- The shard must return raw blocks, so local block assembly can stop before reading every sample.
SELECT min(length(time_series) BETWEEN 1 AND 8192), count()
FROM (SELECT time_series FROM ts_stream_remote LIMIT 5);
SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series))) FROM ts_stream_remote;
SELECT arraySort(groupArray(length(time_series))) FROM ts_stream_remote FINAL;

-- Reading a `TimeSeries` table remotely must also work when plans are sent over the network.
CREATE TABLE ts_stream_roundtrip AS ts_stream_remote
ENGINE = Distributed(test_shard_localhost, currentDatabase(), ts_stream_remote);
SELECT min(length(time_series) BETWEEN 1 AND 8192), count()
FROM (SELECT time_series FROM ts_stream_roundtrip LIMIT 5) SETTINGS serialize_query_plan = 1;
SELECT sum(length(time_series)), sum(arraySum(arrayMap(s -> s.2, time_series)))
FROM ts_stream_roundtrip SETTINGS serialize_query_plan = 1;
DROP TABLE ts_stream_roundtrip;

DROP TABLE ts_stream_remote;
DROP TABLE ts_stream_dist;

DROP TABLE ts_stream_remote_data;
