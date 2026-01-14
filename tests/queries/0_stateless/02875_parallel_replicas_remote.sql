DROP TABLE IF EXISTS tt;
CREATE TABLE tt (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO tt SELECT * FROM numbers(10);

SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

SET enable_parallel_replicas=1, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1;
SELECT count() FROM remote('127.0.0.{1..6}', currentDatabase(), tt) settings log_comment='02875_89f3c39b-1919-48cb-b66e-ef9904e73146';

SYSTEM FLUSH LOGS query_log;

SELECT countIf(ProfileEvents['ParallelReplicasQueryCount']>0) FROM system.query_log
WHERE type = 'QueryFinish' AND event_date >= yesterday()
AND initial_query_id IN (select query_id from system.query_log where current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday() AND log_comment = '02875_89f3c39b-1919-48cb-b66e-ef9904e73146')
SETTINGS parallel_replicas_for_non_replicated_merge_tree=0;

DROP TABLE tt;
