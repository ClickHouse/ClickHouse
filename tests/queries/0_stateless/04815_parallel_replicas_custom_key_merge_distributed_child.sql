-- A `Merge` table over a `Distributed` child plans all of its children up to `WithMergeableState`
-- through an interpreter. The custom-key parallel replicas read replaces a child plan with a remote
-- read at the fixed stage `WithMergeableStateAfterAggregationAndLimit`, so the parent received
-- finalized (post-aggregation, post-LIMIT) data where it expected partial aggregation states:
-- `CANNOT_CONVERT_TYPE` for `count`, and an exception about a missing `AggregatedChunkInfo` in
-- `GroupingAggregatedTransform` when the finalized type coincides with the state type structurally.
-- https://github.com/ClickHouse/ClickHouse/issues/113741

DROP TABLE IF EXISTS t_mrg_ck_1;
DROP TABLE IF EXISTS t_mrg_ck_2;
DROP TABLE IF EXISTS t_mrg_ck_3;

CREATE TABLE t_mrg_ck_1 (k UInt64) ENGINE = MergeTree ORDER BY k AS SELECT number FROM numbers(100000);
CREATE TABLE t_mrg_ck_2 (k UInt64) ENGINE = MergeTree ORDER BY k AS SELECT number FROM numbers(100000);
CREATE TABLE t_mrg_ck_3 (k UInt64) ENGINE = Distributed('test_shard_localhost', currentDatabase(), 't_mrg_ck_1');

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'k';

SELECT count() FROM merge(currentDatabase(), '^t_mrg_ck_');
SELECT sum(k) FROM merge(currentDatabase(), '^t_mrg_ck_') GROUP BY ALL;
SELECT 47, quantileExactInclusive(visibleWidth(['1', '2'])) IGNORE NULLS FROM merge(currentDatabase(), '^t_mrg_ck_') GROUP BY ALL LIMIT 973;

SET parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key_range_upper = 100000;

SELECT count() FROM merge(currentDatabase(), '^t_mrg_ck_');

DROP TABLE t_mrg_ck_1;
DROP TABLE t_mrg_ck_2;
DROP TABLE t_mrg_ck_3;
