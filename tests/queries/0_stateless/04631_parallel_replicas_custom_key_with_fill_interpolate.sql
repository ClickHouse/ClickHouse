-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111724
-- Under custom-key parallel replicas, ORDER BY ... WITH FILL ... INTERPOLATE (...) used to throw
-- LOGICAL_ERROR ("Invalid number of rows in Chunk"): the per-replica read applied WITH FILL and
-- materialized the INTERPOLATE output column, and the initiator re-added it under the same name, so
-- FillingTransform left the duplicate unwritten. WITH FILL now runs only on the initiator over the
-- merged replica streams, so the result matches the non-distributed query. Covers both custom-key
-- modes (sampling and range).
--
-- serialize_query_plan = 0: custom-key parallel replicas rejects serialize_query_plan
-- ("Parallel replicas with custom key are not supported with serialize_query_plan enabled"), and the
-- CI `distributed plan` shard turns it on globally; this test exercises replica dispatch, not plan
-- serialization.

DROP TABLE IF EXISTS t_04631;
CREATE TABLE t_04631 (id UInt32, g UInt16) ENGINE = MergeTree ORDER BY id;
-- Distinct g per row, so the globally-merged fill/interpolation is fully deterministic: g in 0..10,
-- fill synthesizes g = 11..15 with id interpolated from the global predecessor.
INSERT INTO t_04631 SELECT number + 100, number FROM numbers(11);

SET serialize_query_plan = 0;

SELECT g, id FROM t_04631 ORDER BY g WITH FILL FROM 0 TO 16 INTERPOLATE (id AS id + 1)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 2,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_sampling',
    parallel_replicas_custom_key = 'id',
    parallel_replicas_custom_key_range_upper = 200000;

SELECT g, id FROM t_04631 ORDER BY g WITH FILL FROM 0 TO 16 INTERPOLATE (id AS id + 1)
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_range',
    parallel_replicas_custom_key = 'id',
    parallel_replicas_custom_key_range_upper = 200000;

DROP TABLE t_04631;
