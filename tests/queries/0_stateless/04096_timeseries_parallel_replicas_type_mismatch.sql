-- Tags: no-random-settings
-- Regression test for parameter type normalization in timeseries aggregate functions
-- with Parallel Replicas. When parallel_replicas_local_plan=0, coordinator and replica
-- independently reconstruct the aggregate function from query text. Without normalization,
-- constant folding can produce identical numeric values with different Field types
-- (e.g. Int64 vs DecimalField<Decimal64>), causing CANNOT_CONVERT_TYPE errors because
-- Field::operator== compares type discriminators before values.

SET allow_experimental_ts_to_grid_aggregate_function = 1;

-- DateTime (UInt32 timestamp) path
CREATE TABLE ts_pr_dt (timestamp DateTime('UTC'), value Float64) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO ts_pr_dt VALUES
    ('1970-01-01 00:01:41', 10101),
    ('1970-01-01 00:01:57', 10117),
    ('1970-01-01 00:02:11', 10131),
    ('1970-01-01 00:02:15', 10135),
    ('1970-01-01 00:02:31', 10151);

SELECT 'DateTime path - no parallel replicas (baseline):';
SELECT timeSeriesResampleToGridWithStaleness(100, 200, 10, 15)(timestamp, value) FROM ts_pr_dt;

SELECT 'DateTime path - parallel replicas (local_plan=0):';
SELECT timeSeriesResampleToGridWithStaleness(100, 200, 10, 15)(timestamp, value)
FROM clusterAllReplicas('test_shard_localhost', currentDatabase(), ts_pr_dt)
SETTINGS enable_parallel_replicas=1, max_parallel_replicas=3,
    parallel_replicas_for_non_replicated_merge_tree=1,
    parallel_replicas_local_plan=0,
    prefer_localhost_replica=0;

-- DateTime path with CAST parameters (triggers different constant folding)
SELECT 'DateTime path - CAST params - parallel replicas (local_plan=0):';
SELECT timeSeriesResampleToGridWithStaleness(
    CAST(100 AS DateTime('UTC')),
    CAST(200 AS DateTime('UTC')),
    10, 15)(timestamp, value)
FROM clusterAllReplicas('test_shard_localhost', currentDatabase(), ts_pr_dt)
SETTINGS enable_parallel_replicas=1, max_parallel_replicas=3,
    parallel_replicas_for_non_replicated_merge_tree=1,
    parallel_replicas_local_plan=0,
    prefer_localhost_replica=0;

DROP TABLE ts_pr_dt;

-- DateTime64 (Decimal64 timestamp) path
CREATE TABLE ts_pr_dt64 (timestamp DateTime64(3, 'UTC'), value Float64) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO ts_pr_dt64 VALUES
    ('1970-01-01 00:01:41.000', 10101),
    ('1970-01-01 00:01:57.000', 10117),
    ('1970-01-01 00:02:11.000', 10131),
    ('1970-01-01 00:02:15.000', 10135),
    ('1970-01-01 00:02:31.000', 10151);

SELECT 'DateTime64 path - no parallel replicas (baseline):';
SELECT timeSeriesResampleToGridWithStaleness(
    '1970-01-01 00:01:40'::DateTime64(3, 'UTC'),
    '1970-01-01 00:03:20'::DateTime64(3, 'UTC'),
    10::Decimal64(3), 15::Decimal64(3))(timestamp, value)
FROM ts_pr_dt64;

SELECT 'DateTime64 path - parallel replicas (local_plan=0):';
SELECT timeSeriesResampleToGridWithStaleness(
    '1970-01-01 00:01:40'::DateTime64(3, 'UTC'),
    '1970-01-01 00:03:20'::DateTime64(3, 'UTC'),
    10::Decimal64(3), 15::Decimal64(3))(timestamp, value)
FROM clusterAllReplicas('test_shard_localhost', currentDatabase(), ts_pr_dt64)
SETTINGS enable_parallel_replicas=1, max_parallel_replicas=3,
    parallel_replicas_for_non_replicated_merge_tree=1,
    parallel_replicas_local_plan=0,
    prefer_localhost_replica=0;

-- DateTime64 with mixed parameter types (Int literals + DateTime64 CAST)
SELECT 'DateTime64 path - mixed params - parallel replicas (local_plan=0):';
SELECT timeSeriesResampleToGridWithStaleness(
    CAST(100 AS DateTime64(3, 'UTC')),
    CAST(200 AS DateTime64(3, 'UTC')),
    10::Decimal64(3), 15::Decimal64(3))(timestamp, value)
FROM clusterAllReplicas('test_shard_localhost', currentDatabase(), ts_pr_dt64)
SETTINGS enable_parallel_replicas=1, max_parallel_replicas=3,
    parallel_replicas_for_non_replicated_merge_tree=1,
    parallel_replicas_local_plan=0,
    prefer_localhost_replica=0;

DROP TABLE ts_pr_dt64;
