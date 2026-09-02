-- derivative from 03457_move_global_in_to_prewhere

DROP TABLE IF EXISTS 04028_data;
DROP TABLE IF EXISTS 04028_filter;

SET serialize_query_plan=1;
SET enable_parallel_replicas=1, max_parallel_replicas=2, cluster_for_parallel_replicas='test_cluster_one_shard_three_replicas_localhost', parallel_replicas_for_non_replicated_merge_tree=1;
SET parallel_replicas_local_plan = 1;

CREATE TABLE 04028_filter (key UInt64) ENGINE = Memory
AS
SELECT 3 UNION ALL SELECT 23;

CREATE TABLE 04028_data (key UInt64, val String) ENGINE = MergeTree ORDER BY key SETTINGS index_granularity=1;

INSERT INTO 04028_data SELECT number, randomString(2048) FROM numbers(10000);

SYSTEM ENABLE FAILPOINT parallel_replicas_wait_for_unused_replicas;

SELECT key, length(val) FROM (
    SELECT * FROM 04028_data WHERE key GLOBAL IN (04028_filter)
)
ORDER BY key;
