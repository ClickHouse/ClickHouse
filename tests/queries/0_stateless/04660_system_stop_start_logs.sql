-- Tags: no-parallel, no-parallel-replicas
-- SYSTEM STOP/START LOGS changes process-wide logging state; keep it out of parallel runs.

-- Format
SELECT formatQuery('SYSTEM STOP LOGS');
SELECT formatQuery('SYSTEM STOP LOGS WITH FLUSH');
SELECT formatQuery('SYSTEM STOP LOGS query_log, part_log');
SELECT formatQuery('SYSTEM STOP LOGS WITH FLUSH system.query_log');
SELECT formatQuery('SYSTEM START LOGS');
SELECT formatQuery('SYSTEM START LOGS query_log');

-- Unknown log name
SYSTEM STOP LOGS no_such_log_and_never_will_be; -- { serverError BAD_ARGUMENTS }
SYSTEM START LOGS no_such_log_and_never_will_be; -- { serverError BAD_ARGUMENTS }

-- Privilege check
DROP USER IF EXISTS user_04660_stop_start_logs;
CREATE USER user_04660_stop_start_logs;
EXECUTE AS user_04660_stop_start_logs SYSTEM STOP LOGS query_log; -- { serverError ACCESS_DENIED }
EXECUTE AS user_04660_stop_start_logs SYSTEM START LOGS query_log; -- { serverError ACCESS_DENIED }
GRANT SYSTEM LOGS ON *.* TO user_04660_stop_start_logs;
EXECUTE AS user_04660_stop_start_logs SYSTEM STOP LOGS query_log;
EXECUTE AS user_04660_stop_start_logs SYSTEM START LOGS query_log;
DROP USER user_04660_stop_start_logs;

-- Baseline: a marked query is written to query_log
SELECT * FROM system.one WHERE 0 SETTINGS log_comment = '04660_stop_start_logs_before';
SYSTEM FLUSH LOGS query_log;
SELECT count() >= 1 FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '04660_stop_start_logs_before';

-- STOP query_log: new queries must not appear
SYSTEM STOP LOGS query_log;
-- Idempotent
SYSTEM STOP LOGS query_log;

SELECT * FROM system.one WHERE 0 SETTINGS log_comment = '04660_stop_start_logs_stopped', log_queries = 1;
SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '04660_stop_start_logs_stopped';

-- START restores writing
SYSTEM START LOGS query_log;
-- Idempotent
SYSTEM START LOGS query_log;

SELECT * FROM system.one WHERE 0 SETTINGS log_comment = '04660_stop_start_logs_after';
SYSTEM FLUSH LOGS query_log;
SELECT count() >= 1 FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '04660_stop_start_logs_after';

-- START + log_queries=0 still suppresses query_log
SELECT * FROM system.one WHERE 0 SETTINGS log_comment = '04660_stop_start_logs_off', log_queries = 0;
SYSTEM FLUSH LOGS query_log;
SELECT count() FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '04660_stop_start_logs_off';

-- WITH FLUSH: enqueue a query, then stop+flush so it is visible without a separate FLUSH LOGS
SELECT * FROM system.one WHERE 0 SETTINGS log_comment = '04660_stop_start_logs_with_flush';
SYSTEM STOP LOGS WITH FLUSH query_log;
SELECT count() >= 1 FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND log_comment = '04660_stop_start_logs_with_flush';

-- Always restore
SYSTEM START LOGS query_log;
