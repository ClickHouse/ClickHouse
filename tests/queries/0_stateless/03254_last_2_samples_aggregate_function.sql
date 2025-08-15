SET allow_experimental_ts_to_grid_aggregate_function=1;

-- Table for raw data
CREATE TABLE t_raw_timeseries
(
    metric_id UInt64,
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD),
    value Float64 CODEC(DoubleDelta)
)
ENGINE = MergeTree()
ORDER BY (metric_id, timestamp);

-- Table with data resmapled to bigger time steps
CREATE TABLE t_resampled_timeseries
(
    step UInt32,   -- Resampling step in seconds
    metric_id UInt64,
    grid_timestamp DateTime('UTC') CODEC(DoubleDelta, ZSTD),
    samples AggregateFunction(timeSeriesLastTwoSamples, DateTime64(3, 'UTC'), Float64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (step, metric_id, grid_timestamp);

-- MV for populating resampled table
CREATE MATERIALIZED VIEW mv_resampled_timeseries TO t_resampled_timeseries
(
    step UInt32,
    metric_id UInt64,
    grid_timestamp DateTime('UTC') CODEC(DoubleDelta, ZSTD),
    samples AggregateFunction(timeSeriesLastTwoSamples, DateTime64(3, 'UTC'), Float64)
)
AS SELECT *
FROM
(
    SELECT
        10 AS step,
        metric_id,
        ceil(toUnixTimestamp(timestamp + interval 999 millisecond) / step, 0) * step AS grid_timestamp,   -- Round timestamp up to the next grid point
        initializeAggregation('timeSeriesLastTwoSamplesState', timestamp, value) AS samples
    FROM t_raw_timeseries
    ORDER BY metric_id, grid_timestamp
    UNION ALL
    SELECT
        30 AS step,
        metric_id,
        ceil(toUnixTimestamp(timestamp + interval 999 millisecond) / step, 0) * step AS grid_timestamp,   -- Round timestamp up to the next grid point
        initializeAggregation('timeSeriesLastTwoSamplesState', timestamp, value) AS samples
    FROM t_raw_timeseries
    ORDER BY metric_id, grid_timestamp
)
ORDER BY step, metric_id, grid_timestamp
SETTINGS query_plan_remove_redundant_sorting = 0;


-- Table with data resmapled to bigger time steps
-- The difference from t_resampled_timeseries is that we store diff between timestamp and grid_timestamp to improve compression
CREATE TABLE t_resampled_timeseries_delta
(
    step UInt32,   -- Resampling step in seconds
    metric_id UInt64,
    grid_timestamp DateTime('UTC') CODEC(DoubleDelta, ZSTD),
    samples AggregateFunction(timeSeriesLastTwoSamples, Int16, Float64)
)
ENGINE = AggregatingMergeTree()
ORDER BY (step, metric_id, grid_timestamp);

-- MV for populating resampled table
CREATE MATERIALIZED VIEW mv_resampled_timeseries_delta TO t_resampled_timeseries_delta
(
    step UInt32,
    metric_id UInt64,
    grid_timestamp DateTime('UTC') CODEC(DoubleDelta, ZSTD),
    samples AggregateFunction(timeSeriesLastTwoSamples, Int16, Float64)
)
AS SELECT *
FROM
(
    SELECT
        10 AS step,
        metric_id,
        ceil(toUnixTimestamp(timestamp + interval 999 millisecond) / step, 0) * step AS grid_timestamp,   -- Round timestamp up to the next grid point
        initializeAggregation('timeSeriesLastTwoSamplesState', dateDiff('ms', grid_timestamp::DateTime64(3, 'UTC'), timestamp)::Int16, value) AS samples
    FROM t_raw_timeseries
    ORDER BY metric_id, grid_timestamp
    UNION ALL
    SELECT
        30 AS step,
        metric_id,
        ceil(toUnixTimestamp(timestamp + interval 999 millisecond) / step, 0) * step AS grid_timestamp,   -- Round timestamp up to the next grid point
        initializeAggregation('timeSeriesLastTwoSamplesState', dateDiff('ms', grid_timestamp::DateTime64(3, 'UTC'), timestamp)::Int16, value) AS samples
    FROM t_raw_timeseries
    ORDER BY metric_id, grid_timestamp
)
ORDER BY step, metric_id, grid_timestamp
SETTINGS query_plan_remove_redundant_sorting = 0;


-- Insert some data
INSERT INTO t_raw_timeseries(metric_id, timestamp, value) SELECT number%10 AS metric_id, '2024-12-12 12:00:00'::DateTime64(3, 'UTC') + interval ((number/10)%100)*900 millisecond as timestamp, number AS value FROM numbers(1000);

SELECT *
FROM t_raw_timeseries
WHERE metric_id IN (3,7) AND timestamp BETWEEN '2024-12-12 12:00:07' AND '2024-12-12 12:00:13'
ORDER BY metric_id, timestamp;

-- Reload table to test serialization/deserialization of state
DETACH TABLE t_resampled_timeseries;
ATTACH TABLE t_resampled_timeseries;

-- Check resampled data
SELECT metric_id, grid_timestamp, (finalizeAggregation(samples).1 as timestamp, finalizeAggregation(samples).2 as value) 
FROM t_resampled_timeseries
WHERE step = 10 AND metric_id IN (3,7) AND grid_timestamp BETWEEN '2024-12-12 12:00:00' AND '2024-12-12 12:02:00'
ORDER BY metric_id, grid_timestamp;

SELECT metric_id, grid_timestamp, (finalizeAggregation(samples).1 as timestamp, finalizeAggregation(samples).2 as value), arrayMap(x -> grid_timestamp + interval x millisecond, timestamp) as ts
FROM clusterAllReplicas('test_shard_localhost', currentDatabase(),t_resampled_timeseries_delta)
WHERE step = 10 AND metric_id IN (3,7) AND grid_timestamp BETWEEN '2024-12-12 12:00:00' AND '2024-12-12 12:02:00'
ORDER BY metric_id, grid_timestamp
SETTINGS enable_parallel_replicas=1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1;

-- Reload table to test serialization/deserialization of state
DETACH TABLE t_resampled_timeseries_delta;
ATTACH TABLE t_resampled_timeseries_delta;

SELECT step, count(distinct(metric_id)), count(), min(grid_timestamp), max(grid_timestamp) FROM t_resampled_timeseries FINAL GROUP BY step ORDER BY step;

-- Compare aggregated table with timestamps and aggregated table with timestamp deltas
SELECT step, count(), sum(ts1 == ts2)
FROM (
  SELECT
    t.step as step,
    finalizeAggregation(t.samples).1 as ts1,
    arrayMap(x -> t_delta.grid_timestamp + interval x millisecond, finalizeAggregation(t_delta.samples).1) as ts2
  FROM t_resampled_timeseries as t FINAL
  JOIN t_resampled_timeseries_delta as t_delta FINAL
    ON t.step == t_delta.step AND t.metric_id == t_delta.metric_id AND t.grid_timestamp == t_delta.grid_timestamp
)
GROUP BY step
ORDER BY step;
