-- `clusterAllReplicas` reads every replica as a shard of its own, so there is nothing left for parallel
-- replicas to split, and the shard number it ships indexes that numbering rather than the cluster of the
-- same name. Parallel replicas must not be propagated into its sub-queries.

DROP TABLE IF EXISTS t_cluster_all_replicas;
CREATE TABLE t_cluster_all_replicas (n UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_cluster_all_replicas SELECT * FROM numbers(10);

SET automatic_parallel_replicas_mode = 0;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, parallel_replicas_for_non_replicated_merge_tree = 1;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
-- Every replica gets a sub-query of its own.
SET prefer_localhost_replica = 0;

SELECT count() FROM clusterAllReplicas(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t_cluster_all_replicas)
SETTINGS log_comment = '05339_cluster_all_replicas';

SYSTEM FLUSH LOGS query_log;

-- The setting is recorded only when it differs from the default, so a sub-query that ran without parallel
-- replicas has either no entry for it or `0`, when a profile enables them.
SELECT count(), countIf(Settings['allow_experimental_parallel_reading_from_replicas'] NOT IN ('', '0'))
FROM system.query_log
WHERE type = 'QueryFinish' AND NOT is_initial_query AND event_date >= yesterday()
    AND initial_query_id IN (
        SELECT query_id FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
            AND event_date >= yesterday() AND log_comment = '05339_cluster_all_replicas');

DROP TABLE t_cluster_all_replicas;
