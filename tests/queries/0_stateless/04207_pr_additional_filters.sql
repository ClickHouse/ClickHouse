-- Tags: no-parallel
-- ^ failpoint

-- Regression: `additional_table_filters` keys forwarded to parallel-replica secondaries
-- are unqualified by default; the secondary's session current database is not the
-- initiator's, so the planner's table-name match drops the filter. With
-- `parallel_replicas_support_projection` the secondary's `ReadFromMergeTree` then picks
-- the implicit `_minmax_count_projection` and answers per-part instead of per assigned
-- mark range, producing N*total instead of the filtered count.
--
-- The failpoint disables cancellation of unused replicas after all ranges
-- assigned, so every replica's contribution lands on the initiator and the
-- missing-filter regression is deterministic regardless of timing.

DROP TABLE IF EXISTS atf_p;
CREATE TABLE atf_p (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO atf_p SELECT number FROM numbers(10);

SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;
SELECT count() FROM atf_p SETTINGS additional_table_filters = {'atf_p': 'x <= 2'},
    enable_analyzer = 1,
    enable_parallel_replicas = 1,
    automatic_parallel_replicas_mode = 0,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    parallel_replicas_local_plan = 1;

DROP TABLE atf_p;
