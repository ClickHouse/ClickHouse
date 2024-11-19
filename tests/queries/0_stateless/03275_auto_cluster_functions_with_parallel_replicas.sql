-- Tags: no-fasttest
-- Tag no-fasttest: Depends on AWS

SET enable_parallel_replicas=1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1, cluster_for_parallel_replicas='test_cluster_two_shard_three_replicas_localhost';
SELECT * FROM s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', '', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64', 'auto') ORDER BY c1, c2, c3 SETTINGS log_comment='03275_16cb4bb2-813a-43c2-8956-fa3520454020_parallel_replicas';

SET enable_parallel_replicas=0;
SELECT * FROM s3('http://localhost:11111/test/{a,b,c}.tsv', 'test', 'testtest', '', 'TSV', 'c1 UInt64, c2 UInt64, c3 UInt64', 'auto') ORDER BY c1, c2, c3 SETTINGS log_comment='03275_16cb4bb2-813a-43c2-8956-fa3520454020_single_replica';

SYSTEM FLUSH LOGS;

SET enable_parallel_replicas=0;
SET max_rows_to_read = 0; -- system.text_log can be really big
SELECT count() > 0 FROM system.text_log
WHERE query_id in (select query_id from system.query_log where current_database = currentDatabase() and log_comment like '03275_16cb4bb2-813a-43c2-8956-fa3520454020%')
    AND message LIKE '%Parallel reading from replicas is disabled for cluster%';
