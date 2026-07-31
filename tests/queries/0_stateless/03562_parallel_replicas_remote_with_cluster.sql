DROP TABLE IF EXISTS tt;
CREATE TABLE tt (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO tt SELECT * FROM numbers(10);

SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

-- when the query plan is serialized for distributed query, parallel replicas are not enabled because
-- (with prefer_localhost_replica) because all reading steps are ReadFromTable instead of ReadFromMergeTree
SET serialize_query_plan = 0;

SET enable_parallel_replicas=1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1;
SELECT sum(n) FROM remote(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), tt) settings log_comment='03562_8a6a4b56-b9fa-4f60-b201-b637056a89c5';
SELECT sum(n) FROM remote(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), tt) settings log_comment='03562_152a0cc0-0811-46c9-839e-0f17426a1fc6';

SYSTEM FLUSH LOGS query_log;

SELECT countIf(ProfileEvents['ParallelReplicasQueryCount']>0) FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday()
AND initial_query_id IN (select query_id from system.query_log where current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment = '03562_8a6a4b56-b9fa-4f60-b201-b637056a89c5')
SETTINGS parallel_replicas_for_non_replicated_merge_tree=0;

SELECT countIf(ProfileEvents['ParallelReplicasQueryCount']>0) FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday()
AND initial_query_id IN (select query_id from system.query_log where current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment = '03562_152a0cc0-0811-46c9-839e-0f17426a1fc6')
SETTINGS parallel_replicas_for_non_replicated_merge_tree=0;

DROP TABLE tt;
