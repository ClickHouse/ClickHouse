-- Tags: no-old-analyzer

-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/111555 (LIMIT BY half).
-- Under custom-key parallel replicas each replica applied LIMIT n BY per its own row subset and the
-- initiator did not re-apply it, so LIMIT 2 BY g returned up to 2 * replicas rows per group. LIMIT BY
-- is now re-applied on the finalizing initiator over the merged stream, so the result matches the
-- non-distributed query. Covers both custom-key modes; OFFSET is deferred to the initiator.
-- no-old-analyzer: the fix is in the analyzer planner; the deprecated legacy interpreter is unchanged.
--
-- serialize_query_plan = 0 because custom-key parallel replicas reject serialize_query_plan
-- ("Parallel replicas with custom key are not supported with serialize_query_plan enabled") and the
-- CI `distributed plan` shard turns it on globally.

DROP TABLE IF EXISTS t_04657;
CREATE TABLE t_04657 (g UInt16, k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_04657 SELECT number % 3, number FROM numbers(30);

SET serialize_query_plan = 0;

-- Only g is selected, so which rows LIMIT BY keeps is irrelevant; the output is 2 (then 1) g's per group.
SELECT g FROM t_04657 ORDER BY g LIMIT 2 BY g
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_range',
    parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_upper = 30;

SELECT g FROM t_04657 ORDER BY g LIMIT 2 BY g
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_sampling',
    parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_upper = 30;

-- OFFSET must be applied once, on the initiator (one row per group).
SELECT g FROM t_04657 ORDER BY g LIMIT 1, 1 BY g
SETTINGS enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_mode = 'custom_key_range',
    parallel_replicas_custom_key = 'k',
    parallel_replicas_custom_key_range_upper = 30;

DROP TABLE t_04657;
