-- Tags: distributed

-- Regression test for duplicate ALIAS columns expanding to the same expression queried with
-- GROUP BY under parallel replicas. Before the fix the duplicate columns collapsed by name on
-- the replica and the position-based column match threw NUMBER_OF_COLUMNS_DOESNT_MATCH.
-- The bug lives in the analyzer's parallel-replicas query preparation, so force the new analyzer.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;

DROP TABLE IF EXISTS local_pr_alias_dedup;

CREATE TABLE local_pr_alias_dedup
(
    `dt` DateTime,
    `x` UInt8,
    `a1` String ALIAS toString(x),
    `a2` String ALIAS toString(x)
)
ENGINE = MergeTree()
ORDER BY dt;

INSERT INTO local_pr_alias_dedup VALUES ('2024-01-01 00:00:00', 7), ('2024-01-02 00:00:00', 9), ('2024-01-03 00:00:00', 11);

-- Direct parallel-replicas read of a MergeTree table.
SELECT a1, a2 FROM local_pr_alias_dedup GROUP BY a1, a2 ORDER BY a1;
SELECT a1, a2 FROM local_pr_alias_dedup ORDER BY a2;
SELECT a1, a2, count() AS c FROM local_pr_alias_dedup GROUP BY a1, a2 HAVING a2 = '7' ORDER BY a1;

-- remote() over the same table, still with parallel replicas underneath.
SELECT a1, a2 FROM remote('127.0.0.1', currentDatabase(), local_pr_alias_dedup) GROUP BY a1, a2 ORDER BY a1;

DROP TABLE local_pr_alias_dedup;
