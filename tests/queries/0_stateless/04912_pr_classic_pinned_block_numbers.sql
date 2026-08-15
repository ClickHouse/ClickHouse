-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
-- no-replicated-database: the fixture needs exactly two replicas of one table
-- no-shared-merge-tree: SharedMergeTree has no insert quorum
-- no-async-insert: a quorum insert needs insert_quorum_parallel

SET insert_keeper_fault_injection_probability = 0;

DROP TABLE IF EXISTS t_pinned_pr_r1 SYNC;
DROP TABLE IF EXISTS t_pinned_pr_r2 SYNC;

CREATE TABLE t_pinned_pr_r1 (n UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04912/t', 'r1')
ORDER BY n
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0;

CREATE TABLE t_pinned_pr_r2 (n UInt64)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/test_04912/t', 'r2')
ORDER BY n
SETTINGS index_granularity = 128, min_bytes_for_wide_part = 0;

-- Merges are stopped before the inserts, so the part set below is exactly what the
-- arms measure.
SYSTEM STOP MERGES t_pinned_pr_r1;
SYSTEM STOP MERGES t_pinned_pr_r2;

-- Four committed parts: enough marks that the coordinator distributes ranges over
-- more than one replica. A single-part table hides the bug, because every range is
-- then assigned to one replica and the pinned boundary is never crossed.
INSERT INTO t_pinned_pr_r1 SELECT number FROM numbers(3000);
INSERT INTO t_pinned_pr_r1 SELECT number + 1000000 FROM numbers(3000);
INSERT INTO t_pinned_pr_r1 SELECT number + 2000000 FROM numbers(3000);
INSERT INTO t_pinned_pr_r1 SELECT number + 3000000 FROM numbers(3000);

SYSTEM SYNC REPLICA t_pinned_pr_r2;

SYSTEM STOP FETCHES t_pinned_pr_r2;

-- With fetches stopped on r2 the quorum is unsatisfiable, so the insert errors out
-- while the part lands locally on r1 and /quorum/status stays set for it.
-- getMaxAddedBlocks then clamps the partition to that part's max_block - 1, so
-- select_sequential_consistency = 1 sees only the four committed parts.
-- max_insert_threads = 1 keeps a single sink: a second sink would race the first
-- one's /quorum/status and throw UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE instead.
INSERT INTO t_pinned_pr_r1 SELECT number + 9000000 FROM numbers(3000)
SETTINGS insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 2000,
         max_insert_threads = 1; -- { serverError UNKNOWN_STATUS_OF_INSERT,TIMEOUT_EXCEEDED }

-- Preconditions. Without these the arms below are vacuous.
SELECT 'parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_pinned_pr_r1' AND active;

SELECT 'quorum_status', count() FROM system.zookeeper
WHERE path = '/clickhouse/tables/' || currentDatabase() || '/test_04912/t/quorum' AND name = 'status';

SELECT 'local_unpinned', count() FROM t_pinned_pr_r1
SETTINGS select_sequential_consistency = 0, optimize_trivial_count_query = 0, enable_parallel_replicas = 0;

SELECT 'local_pinned', count() FROM t_pinned_pr_r1
SETTINGS select_sequential_consistency = 1, optimize_trivial_count_query = 0, enable_parallel_replicas = 0;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_mode = 0, parallel_replicas_min_number_of_rows_per_replica = 0;
SET optimize_trivial_count_query = 0, parallel_replicas_plan_based = 0;
SET select_sequential_consistency = 1;

-- Every arm must agree with the pinned ground truth above. Before the fix each
-- replica ran a complete local pinned read, so the results were multiplied by the
-- number of participating replicas. count()/sum() detect that; min()/max() do not.
SELECT 'pr_count', count() FROM t_pinned_pr_r1;

SELECT 'pr_sum', sum(n) FROM t_pinned_pr_r1
SETTINGS parallel_replicas_local_plan = 0, parallel_replicas_prefer_local_replica = 0,
         optimize_use_implicit_projections = 0, log_comment = 'test_04912_pinned_pr';

SELECT 'pr_sum_local_plan', sum(n) FROM t_pinned_pr_r1
SETTINGS parallel_replicas_local_plan = 1, parallel_replicas_prefer_local_replica = 0,
         optimize_use_implicit_projections = 0;

-- Control: the plan-based path is not disturbed. It has its own handling of a pinned
-- read, so pin the two settings that select between its variants rather than letting
-- the runner randomize them.
SELECT 'pr_sum_plan_based', sum(n) FROM t_pinned_pr_r1
SETTINGS parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 1,
         parallel_replicas_prefer_local_replica = 1, optimize_use_implicit_projections = 0;

SELECT 'pr_count_unpinned', count() FROM t_pinned_pr_r1
SETTINGS select_sequential_consistency = 0;

-- The pinned read must actually be coordinated, not merely correct: it was 0 before
-- the fix. Immune to how the plan-based path chooses to handle the same pin.
SYSTEM FLUSH LOGS query_log;
SELECT 'coordinated', ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'test_04912_pinned_pr'
  AND type = 'QueryFinish' AND query_id = initial_query_id
SETTINGS enable_parallel_replicas = 0;

SYSTEM START FETCHES t_pinned_pr_r2;
DROP TABLE t_pinned_pr_r1 SYNC;
DROP TABLE t_pinned_pr_r2 SYNC;
