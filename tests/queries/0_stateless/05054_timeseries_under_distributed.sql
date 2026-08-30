-- Tags: no-parallel
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

SELECT '--- a point lookup resolves on whichever shard holds it ---';
SELECT metric_name, tags['host'] FROM ts_dist WHERE metric_name = 'metric_7';

SELECT '--- aggregation runs across both shards ---';
SELECT count(), uniqExact(tags['host']) FROM ts_dist;

SELECT '--- the TimeSeries table functions need a real TimeSeries table, not a Distributed one ---';
SELECT count() > 0 FROM timeSeriesData('shard_0', 'ts_local');
SELECT count() FROM timeSeriesData(currentDatabase(), 'ts_dist'); -- { serverError UNEXPECTED_TABLE_ENGINE }
SELECT count() FROM timeSeriesSelector(currentDatabase(), 'ts_dist', 'up', 0, 0); -- { serverError UNEXPECTED_TABLE_ENGINE }

DROP TABLE ts_dist;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
