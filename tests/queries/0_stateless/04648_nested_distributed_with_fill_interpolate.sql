-- Tags: shard

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111728 (nested-distributed
-- variant). StorageMerge over a Distributed table makes an intermediate node see
-- from_stage = WithMergeableStateAfterAggregation(AndLimit) while to_stage = WithMergeableState. A
-- per-node WITH FILL / INTERPOLATE (the bug) then either throws "Invalid number of rows in Chunk"
-- (analyzer) or duplicates the synthesized fill rows once per node (old analyzer). WITH FILL must run
-- only on the finalizing node over the merged stream, so the result matches the plain query. The two
-- shards read the same table, so real rows are doubled while the fill rows (g = 0 and g = 4) appear once.
--
-- The WITH FILL query is wrapped in an outer ORDER BY g, id: StorageMerge over Distributed does not
-- guarantee a stable row order across configurations, so the outer sort makes the reference
-- deterministic. The bug still shows through as a different multiset (per-node filling duplicates the
-- fill rows: 12 rows instead of 10) or as the logical error, both independent of ordering.
--
-- serialize_query_plan = 0 because WITH FILL is not supported in serialized sort descriptions
-- (serializeSortDescription throws NOT_IMPLEMENTED) and the CI `distributed plan` shard turns
-- serialize_query_plan on globally; this test exercises the (nested) shard read, not plan serialization.

DROP TABLE IF EXISTS mt_04648;
DROP TABLE IF EXISTS dist_04648;
DROP TABLE IF EXISTS merge_04648;

CREATE TABLE mt_04648 (id UInt32, g UInt16) ENGINE = MergeTree ORDER BY id;
INSERT INTO mt_04648 VALUES (10, 1), (20, 2), (30, 3), (40, 5);
CREATE TABLE dist_04648 ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), mt_04648);
CREATE TABLE merge_04648 ENGINE = Merge(currentDatabase(), '^dist_04648$');

SET serialize_query_plan = 0;
SET prefer_localhost_replica = 0;

SELECT g, id FROM
(
    SELECT g, id FROM merge_04648 ORDER BY g WITH FILL FROM 0 TO 6 INTERPOLATE (id AS id + 1)
)
ORDER BY g, id;

DROP TABLE merge_04648;
DROP TABLE dist_04648;
DROP TABLE mt_04648;
