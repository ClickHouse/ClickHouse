-- Confirms parallel replicas respects a row policy on the default path (serialize_query_plan=0),
-- where each replica receives the query as an AST, re-plans it and re-applies its own row policy.
-- The test also proves the query actually ran on the parallel-replicas path (not a silent local
-- fallback) by asserting ParallelReplicasUsedCount > 0 via system.query_log.
-- Checked for both parallel_replicas_local_plan modes: 1 (initiator is a local replica) and 0
-- (all reading on remote replicas). count()+sum(x): a row policy disables trivial-count, so the
-- query forces a real read, and sum(x) proves the rows themselves were filtered.

SET enable_analyzer = 1;                   -- required for parallel replicas
SET serialize_query_plan = 0;              -- default path: AST shipped to replicas
SET automatic_parallel_replicas_mode = 0;  -- don't let randomized auto-PR interfere
SET enable_parallel_replicas = 2,          -- 2 = throw if PR can't be used (no silent local fallback)
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (x UInt8) ENGINE = MergeTree ORDER BY x;
INSERT INTO t VALUES (1), (2), (3);

DROP ROW POLICY IF EXISTS filter ON t;
CREATE ROW POLICY filter ON t USING (x % 2 = 1) TO ALL;  -- only odd x -> {1, 3}

-- Row policy must be applied on the replicas: expect 2 / 4 (not 3 / 6).
SELECT count(), sum(x) FROM t
    SETTINGS parallel_replicas_local_plan = 1, log_comment = '04538_local_plan_1';
SELECT count(), sum(x) FROM t
    SETTINGS parallel_replicas_local_plan = 0, log_comment = '04538_local_plan_0';

SYSTEM FLUSH LOGS query_log;

-- Prove both queries actually ran on the parallel-replicas path.
SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0 FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND event_time >= now() - toIntervalMinute(30)
    AND log_comment = '04538_local_plan_1' AND is_initial_query = 1
    AND current_database = currentDatabase()
ORDER BY event_time DESC LIMIT 1 SETTINGS enable_parallel_replicas = 0;

SELECT ProfileEvents['ParallelReplicasUsedCount'] > 0 FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
    AND event_time >= now() - toIntervalMinute(30)
    AND log_comment = '04538_local_plan_0' AND is_initial_query = 1
    AND current_database = currentDatabase()
ORDER BY event_time DESC LIMIT 1 SETTINGS enable_parallel_replicas = 0;

-- The runtime replica count cannot prove the mixed local+remote branch on a single-node cluster:
-- the in-process local replica grabs the whole working set, so ParallelReplicasUsedCount stays 1
-- regardless of granularity. Assert the plan structure instead. With local_plan = 1 the plan is a
-- union of the initiator's local read (ReadFromMergeTree) AND the remote replicas
-- (ReadFromRemoteParallelReplicas); with local_plan = 0 only the remote read is present.
SELECT countIf(explain LIKE '%ReadFromMergeTree%') > 0,
       countIf(explain LIKE '%ReadFromRemoteParallelReplicas%') > 0
FROM (EXPLAIN SELECT count(), sum(x) FROM t SETTINGS parallel_replicas_local_plan = 1);
SELECT countIf(explain LIKE '%ReadFromMergeTree%') > 0,
       countIf(explain LIKE '%ReadFromRemoteParallelReplicas%') > 0
FROM (EXPLAIN SELECT count(), sum(x) FROM t SETTINGS parallel_replicas_local_plan = 0);

DROP ROW POLICY filter ON t;
DROP TABLE t;
