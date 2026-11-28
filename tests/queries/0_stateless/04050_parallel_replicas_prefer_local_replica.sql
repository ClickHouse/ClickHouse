-- Tags: replica

-- Verify that `parallel_replicas_prefer_local_replica` controls whether parallel replicas
-- are used when `max_parallel_replicas` = 1.

-- Disable automatic parallel replicas mode so randomized settings do not interfere.
SET automatic_parallel_replicas_mode = 0;

-- Keep test stable across analyzer-enabled and analyzer-disabled CI runs.
SET parallel_replicas_only_with_analyzer = 0;

DROP TABLE IF EXISTS t;

CREATE TABLE t(key UInt64, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t SELECT number, toString(number) FROM numbers(1000);

-- Use `sum(key)` instead of bare `count()` to avoid both the trivial count optimization
-- and the minmax_count_proj projection, which would bypass the parallel replicas code path.

-- With `prefer_local_replica` = 0 and `max_parallel_replicas` = 1, the query should
-- use the parallel replicas path (the query is sent to a replica selected
-- by load balancing, not necessarily the local one).
SELECT count(), sum(key)
FROM t
SETTINGS
    enable_parallel_replicas = 1,
    max_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_prefer_local_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    optimize_use_projections = 0,
    log_comment = '04050_prefer_local_0_max_1';

-- With `prefer_local_replica` = 0 and `max_parallel_replicas` = 2, the query should
-- also work and use 2 replicas (but local is not guaranteed to be among them).
SELECT count(), sum(key)
FROM t
SETTINGS
    enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_prefer_local_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    optimize_use_projections = 0,
    log_comment = '04050_prefer_local_0_max_2';

-- Default behavior (`prefer_local_replica` = 1) with `max_parallel_replicas` = 1
-- should NOT use parallel replicas (backward compatibility).
SELECT count(), sum(key)
FROM t
SETTINGS
    enable_parallel_replicas = 1,
    max_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_prefer_local_replica = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    optimize_use_projections = 0,
    log_comment = '04050_prefer_local_1_max_1';

-- INSERT SELECT: with `prefer_local_replica` = 0 and `max_parallel_replicas` = 1,
-- the INSERT SELECT should also use the parallel replicas path.
-- Force `allow_experimental_analyzer` = 1 because `buildInsertSelectPipelineParallelReplicas`
-- has a hard gate on the analyzer, independent of `parallel_replicas_only_with_analyzer`.
DROP TABLE IF EXISTS t_dest;
CREATE TABLE t_dest(key UInt64, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO t_dest
SELECT key, value
FROM t
SETTINGS
    allow_experimental_analyzer = 1,
    enable_parallel_replicas = 1,
    max_parallel_replicas = 1,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_prefer_local_replica = 0,
    parallel_replicas_for_non_replicated_merge_tree = 1,
    log_comment = '04050_insert_select_prefer_local_0_max_1';

SELECT count() FROM t_dest;

SYSTEM FLUSH LOGS query_log;

-- Verify: `prefer_local` = 0, `max_replicas` = 1 should use parallel replicas
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND type = 'QueryFinish'
    AND log_comment = '04050_prefer_local_0_max_1'
    AND is_initial_query = 1
    AND current_database = currentDatabase()
ORDER BY event_time DESC
LIMIT 1;

-- Verify: `prefer_local` = 0, `max_replicas` = 2 should use parallel replicas
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND type = 'QueryFinish'
    AND log_comment = '04050_prefer_local_0_max_2'
    AND is_initial_query = 1
    AND current_database = currentDatabase()
ORDER BY event_time DESC
LIMIT 1;

-- Verify: `prefer_local` = 1, `max_replicas` = 1 should NOT use parallel replicas
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND type = 'QueryFinish'
    AND log_comment = '04050_prefer_local_1_max_1'
    AND is_initial_query = 1
    AND current_database = currentDatabase()
ORDER BY event_time DESC
LIMIT 1;

-- Verify: INSERT SELECT with `prefer_local` = 0, `max_replicas` = 1 should use parallel replicas
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE event_date >= yesterday()
    AND type = 'QueryFinish'
    AND log_comment = '04050_insert_select_prefer_local_0_max_1'
    AND is_initial_query = 1
    AND current_database = currentDatabase()
ORDER BY event_time DESC
LIMIT 1;

DROP TABLE t;
DROP TABLE t_dest;
