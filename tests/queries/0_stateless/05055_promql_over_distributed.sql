-- Tags: no-parallel, no-fasttest, no-flaky-check
-- no-parallel: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases.
-- no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- Raw samples are selected on every shard and the PromQL is evaluated on the initiator, so the answer
-- must be the same as evaluating it against one local TimeSeries table holding all of the data.

SET allow_experimental_time_series_table = 1;
SET distributed_foreground_insert = 1;
SET session_timezone = 'UTC'; -- the reference contains rendered DateTime64 values

DROP TABLE IF EXISTS ts_dist;
DROP TABLE IF EXISTS ts_all;
DROP TABLE IF EXISTS ts_one_shard;
DROP TABLE IF EXISTS ts_remote;
DROP TABLE IF EXISTS ts_remote_tf;
DROP TABLE IF EXISTS ts_nested;
DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;

CREATE DATABASE shard_0;
CREATE DATABASE shard_1;
CREATE TABLE shard_0.ts_local ENGINE = TimeSeries;
CREATE TABLE shard_1.ts_local ENGINE = TimeSeries;

-- Sharded on the host tag, not on metric_name: the key has to split the series of one metric, otherwise
-- every aggregation below would be answerable by a single shard on its own.
CREATE TABLE ts_dist AS shard_0.ts_local
    ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ts_local, cityHash64(tags['host']));

-- h1 and h2 hash to one shard, h3 h4 h5 to the other, so both jobs of `m` straddle the two shards.
INSERT INTO ts_dist (metric_name, tags, time_series) VALUES
    ('m', map('job', 'a', 'host', 'h1'), [(toDateTime64(100, 3), 1), (toDateTime64(110, 3), 2), (toDateTime64(120, 3), 3), (toDateTime64(130, 3), 4), (toDateTime64(140, 3), 5)]),
    ('m', map('job', 'a', 'host', 'h3'), [(toDateTime64(100, 3), 10), (toDateTime64(110, 3), 20), (toDateTime64(120, 3), 30), (toDateTime64(130, 3), 40), (toDateTime64(140, 3), 50)]),
    ('m', map('job', 'b', 'host', 'h2'), [(toDateTime64(100, 3), 100), (toDateTime64(110, 3), 200), (toDateTime64(120, 3), 300), (toDateTime64(130, 3), 400), (toDateTime64(140, 3), 500)]),
    ('m', map('job', 'b', 'host', 'h4'), [(toDateTime64(100, 3), 1000), (toDateTime64(110, 3), 2000), (toDateTime64(120, 3), 3000), (toDateTime64(130, 3), 4000), (toDateTime64(140, 3), 5000)]),
    ('solo', map('host', 'h5'), [(toDateTime64(100, 3), 7), (toDateTime64(110, 3), 8), (toDateTime64(120, 3), 9), (toDateTime64(130, 3), 10), (toDateTime64(140, 3), 11)]);

-- The oracle: the same five series in a single local TimeSeries table.
CREATE TABLE ts_all ENGINE = TimeSeries;
INSERT INTO ts_all (metric_name, tags, time_series) SELECT metric_name, tags, time_series FROM ts_dist;

SELECT '--- both jobs of `m` straddle the two shards, `solo` sits on one of them ---';
SELECT tags['job'] AS job, uniqExact(_shard_num) AS shards FROM ts_dist WHERE metric_name = 'm' GROUP BY job ORDER BY job;
SELECT uniqExact(_shard_num) FROM ts_dist WHERE metric_name = 'solo';

SELECT '--- instant selector ---';
SELECT * FROM prometheusQuery(ts_dist, 'm', 140) ORDER BY ALL;

SELECT '--- rate(): every sample of a series has to reach the same group ---';
SELECT * FROM prometheusQuery(ts_dist, 'rate(m[40s])', 140) ORDER BY ALL;

SELECT '--- sum by (job): one row per job, each totalling series from both shards ---';
SELECT * FROM prometheusQuery(ts_dist, 'sum by (job) (m)', 140) ORDER BY ALL;

SELECT '--- sum(): a single row with the total of all four series ---';
SELECT * FROM prometheusQuery(ts_dist, 'sum(m)', 140) ORDER BY ALL;

SELECT '--- a metric present on one shard only ---';
SELECT * FROM prometheusQuery(ts_dist, 'solo', 140) ORDER BY ALL;

SELECT '--- range query ---';
SELECT * FROM prometheusQueryRange(ts_dist, 'sum by (job) (m)', 120, 140, 10) ORDER BY ALL;

SELECT '--- the distributed answer equals the same PromQL over one local table ---';
SELECT (SELECT groupArray(tuple(*)) FROM (SELECT * FROM prometheusQuery(ts_dist, 'sum by (job) (m)', 140) ORDER BY ALL))
     = (SELECT groupArray(tuple(*)) FROM (SELECT * FROM prometheusQuery(ts_all, 'sum by (job) (m)', 140) ORDER BY ALL));
SELECT (SELECT groupArray(tuple(*)) FROM (SELECT * FROM prometheusQueryRange(ts_dist, 'm', 100, 140, 10) ORDER BY ALL))
     = (SELECT groupArray(tuple(*)) FROM (SELECT * FROM prometheusQueryRange(ts_all, 'm', 100, 140, 10) ORDER BY ALL));

-- With '' above each shard reads the table in its own default database; here the database is named.
SELECT '--- a Distributed table naming its target database explicitly ---';
CREATE TABLE ts_one_shard AS ts_all ENGINE = Distributed(test_shard_localhost, currentDatabase(), ts_all);
SELECT * FROM prometheusQuery(ts_one_shard, 'sum by (job) (m)', 140) ORDER BY ALL;

SELECT '--- rejected: the target is not a Distributed table over TimeSeries tables ---';
CREATE TABLE ts_remote AS shard_0.ts_local ENGINE = Remote('127.0.0.1', shard_0, ts_local);
SELECT * FROM prometheusQuery(ts_remote, 'm', 140); -- { serverError UNEXPECTED_TABLE_ENGINE }
CREATE TABLE ts_remote_tf (number UInt64) ENGINE = Remote('127.0.0.1', numbers(10));
SELECT * FROM prometheusQuery(ts_remote_tf, 'm', 140); -- { serverError UNEXPECTED_TABLE_ENGINE }
CREATE TABLE ts_nested AS ts_all ENGINE = Distributed(test_shard_localhost, currentDatabase(), ts_dist);
SELECT * FROM prometheusQuery(ts_nested, 'm', 140); -- { serverError UNEXPECTED_TABLE_ENGINE }

SELECT '--- the TimeSeries table functions still need a real TimeSeries table ---';
SELECT count() FROM timeSeriesData(currentDatabase(), 'ts_dist'); -- { serverError UNEXPECTED_TABLE_ENGINE }
SELECT count() FROM timeSeriesSamples(currentDatabase(), 'ts_dist'); -- { serverError UNEXPECTED_TABLE_ENGINE }
SELECT count() FROM timeSeriesTags(currentDatabase(), 'ts_dist'); -- { serverError UNEXPECTED_TABLE_ENGINE }
SELECT count() FROM timeSeriesMetrics(currentDatabase(), 'ts_dist'); -- { serverError UNEXPECTED_TABLE_ENGINE }
SELECT count() FROM timeSeriesSelector(currentDatabase(), 'ts_dist', 'm', 0, 0); -- { serverError UNEXPECTED_TABLE_ENGINE }

DROP TABLE ts_nested;
DROP TABLE ts_remote_tf;
DROP TABLE ts_remote;
DROP TABLE ts_one_shard;
DROP TABLE ts_all;
DROP TABLE ts_dist;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
