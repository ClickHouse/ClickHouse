SET max_parallel_replicas=3, allow_experimental_parallel_reading_from_replicas=1, use_hedged_requests=0, cluster_for_parallel_replicas='parallel_replicas';

SET parallel_replicas_for_non_replicated_merge_tree = 0;

SELECT event_time FROM system.query_log LIMIT 1 FORMAT Null;
SELECT max(event_time) FROM system.query_log FORMAT Null;

SET parallel_replicas_for_non_replicated_merge_tree = 1;

SELECT event_time FROM system.query_log LIMIT 1 FORMAT Null;
SELECT max(event_time) FROM system.query_log FORMAT Null;
