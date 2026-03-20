-- Tags: no-parallel, no-asan
-- no-parallel - due to usage of fail points
-- no-asan make test too slow

DROP TABLE IF EXISTS test_pr_last_replica_drain;

-- Use granularity of 1 so each row is its own mark, producing many marks and making
-- the round-trip reduction of the last-replica drain optimization clearly observable
CREATE TABLE test_pr_last_replica_drain (k UInt64, v String)
ENGINE = MergeTree()
ORDER BY tuple()
SETTINGS index_granularity = 1;

INSERT INTO test_pr_last_replica_drain SELECT number % 10, toString(number) FROM numbers(100000);

SET enable_analyzer=1,
    enable_parallel_replicas = 2,
    max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_1_shard_2_replicas_1_unavailable',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mark_segment_size = 128;

-- With test_cluster_1_shard_2_replicas_1_unavailable and max_parallel_replicas=2, the two replicas are:
--   replica 0: 127.0.0.1:9000  (available, local)
--   replica 1: 127.0.0.2:1234  (unavailable)
-- The failpoint waits on the first task request until the unavailable replica is detected.
-- At that point isLastReplica() is true, so the coordinator drains all queues in a single response,
-- including ranges that were hash-assigned to the unavailable replica, minimizing round trips.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_unavailable_replica_on_task_request;

SELECT k, count() FROM test_pr_last_replica_drain GROUP By k SETTINGS log_comment = '04031_pr_last_replica_drain_test', max_threads = 4 FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SET enable_parallel_replicas = 0;

-- Verify: 1 unavailable replica detected, and the last replica only needed 3 round trips
-- (1 announcement, 1 request to drain all data, 1 empty request to signal finish (todo: remove it, requires protocol change))
SELECT
    ProfileEvents['ParallelReplicasUnavailableCount'] AS unavailable_count,
    ProfileEvents['ParallelReplicasNumRequests'] AS num_requests
FROM system.query_log
WHERE yesterday() <= event_date
    AND event_time >= now() - interval 15 minutes
    AND log_comment = '04031_pr_last_replica_drain_test'
    AND current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query_id = initial_query_id;

DROP TABLE test_pr_last_replica_drain;
