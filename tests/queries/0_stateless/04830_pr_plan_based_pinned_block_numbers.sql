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

DROP TABLE IF EXISTS quorum1_pr_pinned;
DROP TABLE IF EXISTS quorum2_pr_pinned;

CREATE TABLE quorum1_pr_pinned (n UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/pr_pinned', 'r1') ORDER BY tuple();
CREATE TABLE quorum2_pr_pinned (n UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/pr_pinned', 'r2') ORDER BY tuple();

INSERT INTO quorum1_pr_pinned SELECT number FROM numbers(10);
SYSTEM SYNC REPLICA quorum2_pr_pinned;

-- A quorum insert that times out with the second replica not fetching leaves `/quorum/status` set, so
-- select_sequential_consistency pins the read below the unconfirmed part: the initiator must see
-- (45, 10), not (435, 30).
SET insert_quorum = 2, insert_quorum_parallel = 0, insert_quorum_timeout = 0;
SYSTEM STOP FETCHES quorum2_pr_pinned;
INSERT INTO quorum1_pr_pinned SELECT number + 10 FROM numbers(20); -- { serverError UNKNOWN_STATUS_OF_INSERT }

SET select_sequential_consistency = 1;

-- The pinned read must not be split for parallel replicas.
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

DROP TABLE quorum1_pr_pinned;
DROP TABLE quorum2_pr_pinned;
