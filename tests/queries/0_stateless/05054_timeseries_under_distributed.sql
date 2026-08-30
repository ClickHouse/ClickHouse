-- Tags: no-parallel, no-flaky-check
-- no-parallel: uses the global shard_0 / shard_1 databases of test_cluster_two_shards_different_databases.

SET allow_experimental_time_series_table = 1;
SET distributed_foreground_insert = 1;

DROP TABLE IF EXISTS ts_dist;
DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;

CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

CREATE TABLE shard_0.ts_local ENGINE = TimeSeries;
CREATE TABLE shard_1.ts_local ENGINE = TimeSeries;

-- A TimeSeries table exposes no integer column, so the sharding key has to be an expression.
CREATE TABLE ts_dist AS shard_0.ts_local
    ENGINE = Distributed(test_cluster_two_shards_different_databases, '', ts_local, cityHash64(metric_name));

INSERT INTO ts_dist (metric_name, tags, time_series)
    SELECT concat('metric_', toString(number)),
           map('host', toString(number)),
           [(toDateTime64('2026-01-01 00:00:00', 3), toFloat64(number))]
    FROM numbers(20);

SELECT '--- every row is readable back through the Distributed table ---';
SELECT count(), uniqExact(metric_name) FROM ts_dist;

SELECT '--- and the rows really are split over both shards ---';
SELECT (SELECT count() FROM shard_0.ts_local) + (SELECT count() FROM shard_1.ts_local) = 20 AS all_rows_landed,
       (SELECT count() FROM shard_0.ts_local) > 0 AND (SELECT count() FROM shard_1.ts_local) > 0 AS both_shards_used;

SELECT '--- both shards are really read, not just one answering for everything ---';
SELECT uniqExact(_shard_num) FROM ts_dist;

SELECT '--- the sample payload survives the round trip, not only the identifying columns ---';
SELECT metric_name, tags['host'], time_series FROM ts_dist WHERE metric_name = 'metric_7';

-- Read routing only consults the sharding key when this is on; with it off the query is broadcast.
-- If the key were evaluated differently on read than on write this would prune to the wrong shard
-- and return nothing.
SELECT '--- with shard pruning on, the sharding key picks the shard holding the series ---';
SELECT metric_name FROM ts_dist WHERE metric_name = 'metric_7' SETTINGS optimize_skip_unused_shards = 1;

SELECT '--- an aggregate over the samples themselves is merged across shards ---';
SELECT sum(arraySum(arrayMap(x -> x.2, time_series))) FROM ts_dist;

SELECT '--- the TimeSeries table functions need a real TimeSeries table, not a Distributed one ---';
SELECT count() > 0 FROM timeSeriesData('shard_0', 'ts_local');
SELECT count() FROM timeSeriesData(currentDatabase(), 'ts_dist'); -- { serverError UNEXPECTED_TABLE_ENGINE }
SELECT count() FROM timeSeriesSelector(currentDatabase(), 'ts_dist', 'up', 0, 0); -- { serverError UNEXPECTED_TABLE_ENGINE }

-- The identifier spelling and the prometheusQuery functions resolve the table id in their own
-- branches, so a regression in either would leave the two assertions above green.
SELECT count() FROM timeSeriesData(ts_dist); -- { serverError UNEXPECTED_TABLE_ENGINE }
SELECT count() FROM prometheusQuery(ts_dist, 'up', 0); -- { serverError UNEXPECTED_TABLE_ENGINE }
SELECT count() FROM prometheusQueryRange(currentDatabase(), 'ts_dist', 'up', 0, 0, 1); -- { serverError UNEXPECTED_TABLE_ENGINE }

DROP TABLE ts_dist;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
