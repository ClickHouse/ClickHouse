-- Regression test for the scope of `max_streams_for_union_step*`.
--
-- The narrowing only applies to steps built for SQL `UNION ALL` / `UNION DISTINCT`.
-- `UnionStep` is also created by `ClusterProxy::executeQuery` for distributed
-- queries (one input per shard/replica), by `StorageBuffer`, by projection
-- optimizations, etc. Narrowing those would shuffle streams via `ConcatProcessor`
-- and break the ordering invariants of downstream transforms such as
-- `GroupingAggregatedTransform` (memory-efficient distributed aggregation).
--
-- This test exercises a distributed query with the cap set; the result must be
-- correct (the cap must NOT be applied to the distributed `UnionStep`).

-- Tags: shard

SET max_streams_for_union_step = 1;
SET max_streams_for_union_step_to_max_threads_ratio = 1;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET distributed_aggregation_memory_efficient = 1;
SET max_threads = 1;

-- One row per replica from `system.one`.
SELECT count() FROM remote('127.0.0.{1,2,3,4,5,6,7,8,9,10}', system.one);

-- Memory-efficient distributed aggregation with the two-level threshold reduced
-- to 1 byte. Without the scoping fix, narrowing the distributed `UnionStep`
-- would interleave bucketed streams via `ConcatProcessor`, producing a
-- different aggregation result.
SELECT sum(c) FROM
(
    SELECT count() AS c FROM remote('127.0.0.{1,2,3,4,5,6,7,8,9,10}', numbers(100))
    GROUP BY number % 7
);
