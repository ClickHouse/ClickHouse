DROP TABLE IF EXISTS atf_p;
CREATE TABLE atf_p (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO atf_p SELECT number FROM numbers(10);

-- `additional_table_filters` keys are resolved against the initiator's session current
-- database, so they cannot be reliably matched on parallel-replica followers (the
-- rewritten query qualifies the table and the follower's current database differs).
-- The analyzer rejects the combination instead of silently dropping the filter.
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1; -- { serverError SUPPORT_IS_DISABLED }

-- With `enable_parallel_replicas = 1` (best-effort) parallel replicas is silently
-- disabled and the query runs locally with the filter applied.
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 1,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1;

DROP TABLE atf_p;
