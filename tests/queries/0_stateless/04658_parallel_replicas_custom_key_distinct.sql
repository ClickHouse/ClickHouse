-- Tags: no-old-analyzer

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111555 (DISTINCT half).
-- Under custom-key parallel replicas each replica applied DISTINCT over its own row subset and the
-- initiator did not re-apply it, so SELECT DISTINCT leaked duplicates across replicas whenever the
-- DISTINCT key is not a function of the custom key. With custom_key = 'k' and rows
-- (g, k) = (1, 0), (1, 1), (2, 2), the replicas emit g = 1, 1, 2 and the initiator returned 1,1,2
-- instead of 1,2. DISTINCT is now finalized on the initiator over the merged stream.
-- no-old-analyzer: the fix is in the analyzer planner; the deprecated legacy interpreter is unchanged.
--
-- serialize_query_plan = 0 because custom-key parallel replicas reject serialize_query_plan
-- ("Parallel replicas with custom key are not supported with serialize_query_plan enabled") and the
-- CI `distributed plan` shard turns it on globally.

DROP TABLE IF EXISTS t_04658;
CREATE TABLE t_04658 (g UInt16, k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_04658 VALUES (1, 0), (1, 1), (2, 2);

SET serialize_query_plan = 0;

SELECT DISTINCT g FROM t_04658 ORDER BY g
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_range',
    parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_upper = 3;

SELECT DISTINCT g FROM t_04658 ORDER BY g
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_sampling',
    parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_upper = 3;

DROP TABLE t_04658;
