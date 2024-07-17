-- Tags: zookeeper, no-parallel, no-fasttest

SELECT path, name
FROM system.zookeeper
WHERE path = '/keeper'
ORDER BY path, name
SETTINGS
  insert_keeper_retry_initial_backoff_ms = 1,
  insert_keeper_retry_max_backoff_ms = 20,
  insert_keeper_fault_injection_probability=0.3,
  insert_keeper_fault_injection_seed=4,
  log_comment='02975_system_zookeeper_retries';


SYSTEM FLUSH LOGS;

-- Check that there where zk session failures
SELECT ProfileEvents['ZooKeeperHardwareExceptions'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment='02975_system_zookeeper_retries'
ORDER BY event_time_microseconds DESC
LIMIT 1;
