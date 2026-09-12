-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: `serialize_query_plan` requires the analyzer.

-- A storage that reports `hasEvenlyDistributedRead` (`Memory`, `numbers_mt`, ...) already spreads the
-- rows over the reading streams, so the planner skips the pre-aggregation resize entirely and the
-- gradual/strict choice never arises. The query plan serialization does not carry that property -
-- every deserialized `AggregatingStep` is reconstructed with `storage_has_evenly_distributed_read`
-- unset - so a plan fragment shipped to a shard does reach the resize branch. The gradual-resize
-- mark must therefore not travel with such a step, otherwise the same query would build a
-- `GradualResize` when it is shipped and no resize at all when it is planned locally, i.e. the
-- pipeline shape would depend on the transport.
-- The pipeline of a shipped plan fragment is not visible in `EXPLAIN PIPELINE`, hence the
-- introspection through `processors_profile_log`.

DROP TABLE IF EXISTS test_gradual_resize_evenly_distributed;
CREATE TABLE test_gradual_resize_evenly_distributed (k UInt64, v UInt64) ENGINE = Memory;
INSERT INTO test_gradual_resize_evenly_distributed SELECT number % 10, number FROM numbers(200000);

DROP TABLE IF EXISTS test_gradual_resize_merge_tree;
CREATE TABLE test_gradual_resize_merge_tree (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 256;
INSERT INTO test_gradual_resize_merge_tree SELECT number % 10, number FROM numbers(200000);

SET min_rows_per_stream_for_gradual_resize = 1000;
SET min_bytes_per_stream_for_gradual_resize = 0;
SET max_threads = 4;
-- `max_threads` is silently lowered to the number of threads that fit into the free memory
-- (`getMaxThreadsForAvailableMemory`), which on a loaded CI runner collapses the pipeline to a
-- single stream and removes every resize processor. Pin it off, the assertions below are about
-- the pipeline shape.
SET max_threads_min_free_memory_per_thread = 0;
-- The number of read streams of the `MergeTree` control is capped a second time by the minimum
-- number of marks per concurrent read, which is derived from the randomized `index_granularity_bytes`.
SET merge_tree_min_rows_for_concurrent_read = 0;
SET merge_tree_min_bytes_for_concurrent_read = 0;
-- Aggregation in order takes a different pipeline branch that has no pre-aggregation resize.
SET optimize_aggregation_in_order = 0;
SET log_processors_profiles = 1;

SET serialize_query_plan = 1;
SET prefer_localhost_replica = 0;

SELECT k, sum(v) FROM cluster(test_shard_localhost, currentDatabase(), test_gradual_resize_evenly_distributed)
    GROUP BY k FORMAT Null SETTINGS log_comment = '05182_shipped_evenly_distributed';
-- Positive control: the very same shipped-plan shape over a storage without an evenly distributed
-- read does build the `GradualResize`.
SELECT k, sum(v) FROM cluster(test_shard_localhost, currentDatabase(), test_gradual_resize_merge_tree)
    GROUP BY k FORMAT Null SETTINGS log_comment = '05182_shipped_merge_tree';

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
      AND log_comment IN ('05182_shipped_evenly_distributed', '05182_shipped_merge_tree')
) AS q ON p.initial_query_id = q.query_id
WHERE p.event_date >= yesterday() AND p.event_time >= now() - INTERVAL 10 MINUTE
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE test_gradual_resize_evenly_distributed;
DROP TABLE test_gradual_resize_merge_tree;
