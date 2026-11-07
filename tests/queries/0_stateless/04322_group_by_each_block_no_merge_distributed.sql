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
    SETTINGS group_by_each_block_no_merge = 1, distributed_aggregation_memory_efficient = 0,
             enable_memory_bound_merging_of_aggregation_results = 0, max_block_size = 1000
);

-- Memory-efficient distributed aggregation requires the first stage to emit bucket-ordered
-- intermediate states, which the per-block streaming flush cannot provide: it pushes the chunks
-- of every block directly, bypassing the bucket-ordering protocol of `GroupingAggregatedTransform`.
-- The combination is rejected on both the analyzer and the old-interpreter path.
SELECT sum(c), sum(s) FROM
(
    SELECT number AS k, count() AS c, sum(number) AS s
    FROM cluster('test_cluster_two_shards', numbers(10000)) GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, distributed_aggregation_memory_efficient = 1, group_by_two_level_threshold = 1
); -- { serverError NOT_IMPLEMENTED }

-- The same holds for memory-bound merging of aggregation results.
SELECT sum(c), sum(s) FROM
(
    SELECT number AS k, count() AS c, sum(number) AS s
    FROM cluster('test_cluster_two_shards', numbers(10000)) GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, distributed_aggregation_memory_efficient = 0, enable_memory_bound_merging_of_aggregation_results = 1, optimize_aggregation_in_order = 1
); -- { serverError NOT_IMPLEMENTED }

-- External (on-disk) aggregation is disabled while `group_by_each_block_no_merge` is enabled (only one block
-- is held in memory at a time), so spilling cannot mix data from different blocks. Even with a tiny external
-- threshold the per-block partial results are clean and the totals are still correct (no row is lost and the
-- query does not fail).
SELECT sum(c), sum(s) FROM
(
    SELECT number DIV 113 AS k, count() AS c, sum(number) AS s
    FROM numbers(10000) GROUP BY k
    SETTINGS group_by_each_block_no_merge = 1, max_block_size = 1000, max_bytes_before_external_group_by = 1, max_bytes_ratio_before_external_group_by = 0
);
