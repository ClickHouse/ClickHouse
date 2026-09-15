-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning and `serialize_query_plan` require the analyzer.

-- The decision to skip the gradual pre-aggregation resize for `GROUP BY` keys that are semantically
-- constant (`GROUP BY materialize(1)`: a single group, so throttling saves no merging work) is taken
-- by the planner from the pre-aggregation actions DAG and stored on `AggregatingStep`. A consumer of
-- the step no longer has that DAG, so the decision has to survive both ways a plan is copied:
-- `AggregatingStep::clone` (the distributed Cascades optimizer) and the query plan serialization
-- round-trip (`serialize_query_plan`, where the shard never analyzes the query at all).
-- The pipeline of a shipped plan fragment is not visible in `EXPLAIN PIPELINE`, hence the
-- introspection through `processors_profile_log`.
-- `numbers(...)` reports `hasEvenlyDistributedRead = true` and bypasses the pre-aggregation resize
-- entirely, so the source has to be a `MergeTree` table.

DROP TABLE IF EXISTS test_gradual_resize_plan_copies;
CREATE TABLE test_gradual_resize_plan_copies (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 256;
INSERT INTO test_gradual_resize_plan_copies SELECT number % 10, number FROM numbers(100000);

SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET max_threads = 4;
-- `max_threads` is silently lowered to the number of threads that fit into the free memory
-- (`getMaxThreadsForAvailableMemory`), which on a loaded CI runner collapses the pipeline to a
-- single stream and removes every resize processor. Pin it off, the assertions below are about
-- the pipeline shape.
SET max_threads_min_free_memory_per_thread = 0;
-- The number of read streams is capped a second time by the minimum number of marks per
-- concurrent read, which is derived from `index_granularity_bytes` - a randomized `MergeTree`
-- setting. A small granularity in bytes makes that cap huge, collapses the read to a single stream
-- and removes every resize processor from the pipeline. Pin it off for the same reason.
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;
-- Aggregation in order takes a different pipeline branch that has no pre-aggregation resize.
SET optimize_aggregation_in_order = 0;
SET log_processors_profiles = 1;

-- Carrier 1: the distributed Cascades optimizer enumerates its alternatives on clones of the step.
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 2;

SELECT k, sum(v) FROM test_gradual_resize_plan_copies GROUP BY k
    FORMAT Null SETTINGS log_comment = '05056_cloned_keyed';
SELECT sum(v) FROM test_gradual_resize_plan_copies GROUP BY materialize(1)
    FORMAT Null SETTINGS log_comment = '05056_cloned_constant';

-- Carrier 2: the plan shipped to the shard. The shard does not analyze the query, so the decision
-- can only come from the serialized step.
SET make_distributed_plan = 0;
SET enable_cascades_optimizer = 0;
SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;

SELECT k, sum(v) FROM cluster(test_shard_localhost, currentDatabase(), test_gradual_resize_plan_copies)
    GROUP BY k FORMAT Null SETTINGS log_comment = '05056_serialized_keyed';
SELECT sum(v) FROM cluster(test_shard_localhost, currentDatabase(), test_gradual_resize_plan_copies)
    GROUP BY materialize(1) FORMAT Null SETTINGS log_comment = '05056_serialized_constant';

SET serialize_query_plan = 0;

SYSTEM FLUSH LOGS processors_profile_log, query_log;

-- The `event_time` bound keeps the log scans cheap: without it every flaky-check rerun scans all
-- the log rows accumulated by the earlier runs.
SELECT
    log_comment,
    countIf(name = 'GradualResize') > 0 AS has_gradual_resize
FROM system.processors_profile_log AS p
INNER JOIN
(
    SELECT query_id, log_comment
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 10 MINUTE
      AND current_database = currentDatabase() AND type = 'QueryFinish'
      AND log_comment IN ('05056_cloned_keyed', '05056_cloned_constant',
                          '05056_serialized_keyed', '05056_serialized_constant')
) AS q ON p.initial_query_id = q.query_id
WHERE p.event_date >= yesterday() AND p.event_time >= now() - INTERVAL 10 MINUTE
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE test_gradual_resize_plan_copies;
