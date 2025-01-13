DROP TABLE IF EXISTS tt;
CREATE TABLE tt (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO tt SELECT * FROM numbers(10);

SET enable_parallel_replicas=1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1;
SELECT count() FROM clusterAllReplicas('test_cluster_two_shard_three_replicas_localhost', currentDatabase(), tt) settings log_comment='02875_190aed82-2423-413b-ad4c-24dcca50f65b';

SYSTEM FLUSH LOGS;

SET enable_parallel_replicas=0;
SET max_rows_to_read = 0; -- system.text_log can be really big
SELECT count() > 0 FROM system.text_log
WHERE query_id in (select query_id from system.query_log where current_database = currentDatabase() AND log_comment = '02875_190aed82-2423-413b-ad4c-24dcca50f65b')
    AND message LIKE '%Parallel reading from replicas is disabled for cluster%';

DROP TABLE tt;
