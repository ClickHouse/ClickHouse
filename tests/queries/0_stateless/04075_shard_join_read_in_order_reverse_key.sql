-- Regression test for incorrect ORDER BY elision when query_plan_read_in_order_through_join
-- and query_plan_join_shard_by_pk_ranges are both active on a reverse-key table.
--
-- Root cause: with read_in_order_through_join the optimizer traverses through the JOIN,
-- finds the MergeTree PK (c0 DESC), and demotes the outer ORDER BY from FullSortingStep to
-- FinishSortingStep, trusting the storage will emit rows in PK order end-to-end.
-- optimizeJoinByShards then splits the read into PK-range layers via
-- splitIntersectingPartsRangesIntoLayers, which intentionally includes the border granule in
-- both adjacent layers.  No cross-shard merge is performed for the outer ORDER BY, so each
-- shard emits rows in DESC storage order independently and outputs are concatenated, producing
-- wrong results (e.g. 4,3,2,1,0,9,8,7,6,5 instead of 0..9).
--
-- Fix: optimizeJoinByShards now runs before applyOrder (which calls optimizeReadInOrder).
-- In buildInputOrderInfo, before converting FullSortingStep → FinishSortingStep, we check
-- whether the reading step's analysis result already has split_parts.layers.size() > 1
-- (i.e. sharding actually created multiple layers).  If so, we return nullptr early to
-- preserve the outer ORDER BY as a FullSortingStep.
--
-- The same guard is applied defensively to the AggregatingStep and DistinctStep overloads.
--
-- All settings that gate the bug are pinned so the test is deterministic.
-- Without the fix the first query returns: 4, 3, 2, 1, 0, 9, 8, 7, 6, 5

DROP TABLE IF EXISTS t0;

CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() ORDER BY (c0 DESC)
    SETTINGS index_granularity = 1, allow_experimental_reverse_key = 1;

INSERT INTO t0 (c0) SELECT number FROM numbers(10);

SELECT c0 FROM t0 JOIN t0 tx USING (c0) ORDER BY c0
    SETTINGS
        -- triggers optimizeJoinByShards: splits MergeTree read into overlapping PK-range layers
        query_plan_join_shard_by_pk_ranges = 1,
        -- allows optimizeReadInOrder to traverse through the JOIN and find the MergeTree below,
        -- causing it to demote FullSortingStep -> FinishSortingStep (the incorrect elision)
        query_plan_read_in_order_through_join = 1,
        -- master switch for optimizeReadInOrder
        optimize_read_in_order = 1,
        -- inserts a virtual sentinel row so FinishSortingStep trusts stream order;
        -- without this it falls back to a full sort and accidentally hides the bug
        read_in_order_use_virtual_row = 1,
        -- must be 0: the default runtime Bloom-filter step between MergeTree and JOIN is opaque
        -- to findReadingStep in optimizeJoinByShards, which then silently skips the shard split
        enable_join_runtime_filters = 0,
        -- two threads so shard layer outputs are concatenated without a cross-shard sort
        max_threads = 2,
        -- must be 0: CI chaos injection randomizes range splits and can mask the bug
        merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- buildInputOrderInfo(AggregatingStep) path: aggregation-in-order through JOIN with shard split.
-- MergingAggregatedStep correctly merges partial results from overlapping shard boundaries,
-- so the output is correct even without the guard. The guard is applied defensively to prevent
-- the optimizer from making an ordering assumption that the shard split has already invalidated.
SELECT c0, count() FROM t0 JOIN t0 tx USING (c0) GROUP BY c0 ORDER BY c0
    SETTINGS
        query_plan_join_shard_by_pk_ranges = 1,
        query_plan_read_in_order_through_join = 1,
        optimize_read_in_order = 1,
        optimize_aggregation_in_order = 1,
        query_plan_aggregation_in_order = 1,
        read_in_order_use_virtual_row = 1,
        enable_join_runtime_filters = 0,
        max_threads = 2,
        merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

-- buildInputOrderInfo(DistinctStep) path: distinct-in-order through JOIN with shard split.
-- The final Distinct step deduplicates across shards, so the output is correct even without
-- the guard. The guard is applied defensively for the same reason as AggregatingStep above.
SELECT DISTINCT c0 FROM t0 JOIN t0 tx USING (c0) ORDER BY c0
    SETTINGS
        query_plan_join_shard_by_pk_ranges = 1,
        query_plan_read_in_order_through_join = 1,
        optimize_read_in_order = 1,
        optimize_distinct_in_order = 1,
        read_in_order_use_virtual_row = 1,
        enable_join_runtime_filters = 0,
        max_threads = 2,
        merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;

DROP TABLE t0;
