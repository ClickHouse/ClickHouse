-- Tags: shard

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111728 (clusterAllReplicas
-- variant). Under clusterAllReplicas the per-replica read applied WITH FILL and materialized the
-- INTERPOLATE output column, and the initiator re-added it under the same name, so the initiator
-- Filling step received the interpolate column twice. FillingTransform::insertFromFillingRow bounds
-- its loop by interpolate_block.columns() but indexes interpolate_columns (sized via getPositionByName,
-- which collapses both entries to one slot), so besides "Invalid number of rows in Chunk" this can be
-- an out-of-bounds abort under libc++ hardening / TSan (a single INTERPOLATE target is enough - not the
-- multiple-outputs-collapsing case). WITH FILL now runs only on the finalizing node over the merged
-- stream, so the interpolate column is materialized once and the block stays rectangular.
--
-- The WITH FILL query is wrapped in an outer ORDER BY a, c so the reference is deterministic
-- regardless of how the replica streams interleave. The three replicas read the same table, so real
-- rows are tripled while the fill rows (a = 1, a = 2) are generated once.
--
-- serialize_query_plan = 0 because WITH FILL is not supported in serialized sort descriptions
-- (serializeSortDescription throws NOT_IMPLEMENTED) and the CI `distributed plan` shard turns
-- serialize_query_plan on globally; this test exercises the replica read, not plan serialization.

DROP TABLE IF EXISTS t2_04652;
CREATE TABLE t2_04652 (a UInt64, c UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t2_04652 VALUES (0, 100), (3, 300);

SET serialize_query_plan = 0;

SELECT a, c FROM
(
    SELECT a, c
    FROM clusterAllReplicas('test_cluster_one_shard_three_replicas_localhost', currentDatabase(), 't2_04652')
    ORDER BY a WITH FILL FROM 0 TO 4 STEP 1 INTERPOLATE (c AS c)
)
ORDER BY a, c;

DROP TABLE t2_04652;
