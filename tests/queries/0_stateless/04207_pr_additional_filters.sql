-- Tags: no-parallel
-- ^ failpoint

DROP TABLE IF EXISTS atf_p;
CREATE TABLE atf_p (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO atf_p SELECT number FROM numbers(10);

-- The failpoint disables cancellation of unused replicas after all ranges
-- are assigned, so every replica's contribution lands on the initiator and any
-- missing-filter regression is deterministic regardless of timing.

-- `additional_table_filters` keys are resolved against the initiator's session current
-- database, so they cannot be reliably matched on parallel-replica followers (the
-- rewritten query qualifies the table and the follower's current database differs).
-- On the legacy AST-forwarding path the analyzer rejects the combination instead of
-- silently dropping the filter.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    serialize_query_plan = 0; -- { serverError SUPPORT_IS_DISABLED }

SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 0,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    serialize_query_plan = 0;

-- With `enable_parallel_replicas = 1` (best-effort) parallel replicas is silently
-- disabled and the query runs locally with the filter applied.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 1,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    serialize_query_plan = 0;

-- With `serialize_query_plan = 1` the initiator lowers the additional filter into an
-- explicit `FilterStep` and ships the serialized plan, so the follower never
-- re-resolves the setting and the combination works.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1,
    serialize_query_plan = 1;

-- Verify the additional filter is present in the serialized plan shipped to followers.
SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
EXPLAIN PLAN actions = 1, distributed = 1
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    query_plan_remove_unused_columns = 1,
    parallel_replicas_local_plan = 1,
    serialize_query_plan = 1;

SYSTEM DISABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
DROP TABLE atf_p;
