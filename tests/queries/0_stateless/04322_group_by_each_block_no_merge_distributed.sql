-- Regression tests for the `group_by_each_block_no_merge` streaming GROUP BY setting.
-- The setting is applied only to the inner aggregation via a per-subquery SETTINGS clause,
-- so the outer aggregation can be used to verify totals normally.

-- Finite input: the result is produced per block and is intentionally not merged across blocks,
-- but no row must be lost (in particular the last block must be flushed). The partial group
-- counts still sum up to the total number of rows, and the partial sums to the total sum.
SELECT sum(c), sum(s) FROM
(
    SELECT number DIV 113 AS k, count() AS c, sum(number) AS s
    FROM numbers(10000) GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
);

-- Distributed (two-stage) aggregation used to throw `Bad cast .. to ColumnAggregateFunction`
-- because the first stage produced finalized columns instead of intermediate states.
-- Now the first stage honors `final` and emits states, which are merged on the initiator,
-- so the distributed result is fully merged and correct (both shards read the same data).
SELECT sum(c), sum(s) FROM
(
    SELECT number DIV 113 AS k, count() AS c, sum(number) AS s
    FROM cluster('test_cluster_two_shards', numbers(10000)) GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000
);
