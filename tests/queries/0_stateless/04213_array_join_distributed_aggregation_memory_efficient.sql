-- Regression test for #104289: with ARRAY JOIN over a remote source the new analyzer
-- used to drop the bucket-aware memory-efficient merge and fall back to a single
-- MergingAggregatedTransform on the coordinator (causing OOM at production scale).
-- The trigger was that ARRAY JOIN adds a second entry to PlannerContext's
-- table_expression_node_to_data, so addMergingAggregatedStep's `size() == 1` gate
-- skipped the is_remote_storage detection.

-- Tags: shard

DROP TABLE IF EXISTS t_array_join_remote_agg;
CREATE TABLE t_array_join_remote_agg
(
    k UInt32,
    arr Array(UInt32)
) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_array_join_remote_agg VALUES (1, [10, 20]), (2, [30]);

SET allow_experimental_analyzer = 1;
SET distributed_aggregation_memory_efficient = 1;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;

-- Bucket-aware merging must engage for a remote source even when the query has ARRAY JOIN.
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT k, sum(elem)
    FROM remote('127.0.0.{1,2}', currentDatabase(), t_array_join_remote_agg)
    LEFT ARRAY JOIN arr AS elem
    GROUP BY k
)
WHERE explain LIKE '%MergingAggregatedBucketTransform%';

-- Sanity: the result is correct.
SELECT k, sum(elem) AS s
FROM remote('127.0.0.{1,2}', currentDatabase(), t_array_join_remote_agg)
LEFT ARRAY JOIN arr AS elem
GROUP BY k
ORDER BY k;

DROP TABLE t_array_join_remote_agg;
