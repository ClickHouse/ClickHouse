-- Tags: no-random-settings

DROP TABLE IF EXISTS test_parallel_replicas_settings;
CREATE TABLE test_parallel_replicas_settings (n UInt64) ENGINE=MergeTree() ORDER BY tuple();
INSERT INTO test_parallel_replicas_settings SELECT * FROM numbers(10);

SET enable_parallel_replicas=2, max_parallel_replicas=3, parallel_replicas_for_non_replicated_merge_tree=1;
SET parallel_replicas_only_with_analyzer = 0;  -- necessary for CI run with disabled analyzer

SET cluster_for_parallel_replicas='';
SELECT count() FROM test_parallel_replicas_settings WHERE NOT ignore(*); -- { serverError CLUSTER_DOESNT_EXIST }

SET cluster_for_parallel_replicas='parallel_replicas';
SELECT count() FROM test_parallel_replicas_settings WHERE NOT ignore(*) settings log_comment='0_f621c4f2-4da7-4a7c-bb6d-052c442d0f7f';

SYSTEM FLUSH LOGS text_log, query_log;
SET max_rows_to_read = 0; -- system.text_log can be really big
SELECT count() > 0 FROM system.text_log
WHERE yesterday() <= event_date
      AND query_id in (select query_id from system.query_log where current_database=currentDatabase() AND log_comment='0_f621c4f2-4da7-4a7c-bb6d-052c442d0f7f')
      AND level = 'Information'
      AND message ILIKE '%Disabling ''use_hedged_requests'' in favor of ''enable_parallel_replicas''%'
SETTINGS enable_parallel_replicas=0;

SET use_hedged_requests=1;
SELECT count() FROM test_parallel_replicas_settings WHERE NOT ignore(*) settings log_comment='1_f621c4f2-4da7-4a7c-bb6d-052c442d0f7f';

SYSTEM FLUSH LOGS text_log, query_log;

SET enable_parallel_replicas=0;
SELECT count() > 0 FROM system.text_log
WHERE yesterday() <= event_date
      AND query_id in (select query_id from system.query_log where current_database = currentDatabase() AND log_comment = '1_f621c4f2-4da7-4a7c-bb6d-052c442d0f7f')
      AND level = 'Warning'
      AND message ILIKE '%Setting ''use_hedged_requests'' explicitly with enabled ''enable_parallel_replicas'' has no effect%'
SETTINGS enable_parallel_replicas=0;

DROP TABLE test_parallel_replicas_settings;
