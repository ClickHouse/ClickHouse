-- Tags: zookeeper, no-replicated-database, no-shared-merge-tree, no-async-insert
-- Tag no-replicated-database: the fixture needs exactly two replicas of the table
-- Tag no-shared-merge-tree: no quorum
-- Tag no-async-insert: async inserts with quorum only make sense with insert_quorum_parallel

-- A read pinned to a block-number boundary (select_sequential_consistency clamps the read to the parts
-- the initiator saw confirmed by the insert quorum) must not be shipped into a plan-based
-- parallel-replicas fragment: ReadFromMergeTree::serialize does not ship the pin and deserialize
-- rebuilds the read with max_block_numbers_to_read = nullptr, so a follower replica would silently
-- read newer parts than the initiator's snapshot allows and return rows the quorum never confirmed.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_min_number_of_rows_per_replica = 0;
SET insert_keeper_fault_injection_probability = 0;

DROP TABLE IF EXISTS quorum1_pr_pinned;
DROP TABLE IF EXISTS quorum2_pr_pinned;
DROP TABLE IF EXISTS plain_mt_pr_pinned;

CREATE TABLE quorum1_pr_pinned (n UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/pr_pinned', 'r1') ORDER BY tuple();
CREATE TABLE quorum2_pr_pinned (n UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/pr_pinned', 'r2') ORDER BY tuple();

INSERT INTO quorum1_pr_pinned SELECT number FROM numbers(10);
SYSTEM SYNC REPLICA quorum2_pr_pinned;

-- An unsatisfiable quorum insert with the second replica not fetching leaves `/quorum/status` set, so
-- select_sequential_consistency pins the read below the unconfirmed part: the initiator must see
-- (45, 10), not (435, 30). Either error leaves that node set: the insert reports
-- UNKNOWN_STATUS_OF_INSERT when it waits out its own timeout, and
-- UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE when a status node is already there when it starts.
SET insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 0;
SYSTEM STOP FETCHES quorum2_pr_pinned;
INSERT INTO quorum1_pr_pinned SELECT number + 10 FROM numbers(20); -- { serverError UNKNOWN_STATUS_OF_INSERT,UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE }
SET insert_quorum = 0;

SET select_sequential_consistency = 1;

-- The pinned read must not be split for parallel replicas. There is no join to lift here, so a split
-- either exists or it does not and presence is the whole question.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN pretty=0, description=0 SELECT sum(n) FROM quorum1_pr_pinned WHERE n >= 0);

-- Executing remote-only must still respect the initiator's snapshot boundary: without the guard the
-- followers rebuild the read without the pin and return the 20 rows the quorum never confirmed.
SELECT sum(n), count() FROM quorum1_pr_pinned
SETTINGS parallel_replicas_local_plan = 0, parallel_replicas_prefer_local_replica = 0;

-- The same read without sequential consistency is still split.
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0
FROM (EXPLAIN pretty=0, description=0 SELECT sum(n) FROM quorum1_pr_pinned WHERE n >= 0)
SETTINGS select_sequential_consistency = 0;

-- The same predicate also gates the broadcast side of a lifted join, where the pinned table is read in
-- full by every replica. The coordinated side must therefore be a table that is still eligible under
-- sequential consistency: StorageMergeTree has no sequential-consistency branch, so a plain MergeTree
-- keeps its split marker and the broadcast-side check is reached.
CREATE TABLE plain_mt_pr_pinned (n UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO plain_mt_pr_pinned SELECT number FROM numbers(30);

SET parallel_replicas_for_non_replicated_merge_tree = 1;
-- A randomized join order can put a BuildRuntimeFilter between the join and the coordinated read, and a
-- swap flips which side is coordinated; both would change what is being tested.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';

-- Step order, not step presence: refusing the lift keeps the split below the join, so
-- ReadFromParallelReplicas is in the plan either way and only its position separates the two shapes.
-- Pinned, the join stays above the distributed read; unpinned, the whole join ships and the
-- distributed read is a sibling of the local plan that holds the join.
SELECT arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM plain_mt_pr_pinned AS p JOIN quorum1_pr_pinned AS q ON p.n = q.n)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

SELECT count() FROM plain_mt_pr_pinned AS p JOIN quorum1_pr_pinned AS q ON p.n = q.n
SETTINGS parallel_replicas_local_plan = 0, parallel_replicas_prefer_local_replica = 0;

-- The same join without sequential consistency is still lifted, and matches all 30 rows.
SELECT arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count() FROM plain_mt_pr_pinned AS p JOIN quorum1_pr_pinned AS q ON p.n = q.n)
    WHERE step IN ('Aggregating', 'Union', 'Join', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
) SETTINGS select_sequential_consistency = 0;

SELECT count() FROM plain_mt_pr_pinned AS p JOIN quorum1_pr_pinned AS q ON p.n = q.n
SETTINGS parallel_replicas_local_plan = 0, parallel_replicas_prefer_local_replica = 0, select_sequential_consistency = 0;

DROP TABLE quorum1_pr_pinned;
DROP TABLE quorum2_pr_pinned;
DROP TABLE plain_mt_pr_pinned;
