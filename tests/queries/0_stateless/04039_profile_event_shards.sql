-- Tags: shard

SET send_logs_level = 'fatal';

-- Single distributed query: 2 shards
SELECT count() FROM remote('127.0.0.{1,2}', system.one) FORMAT Null SETTINGS log_comment = '04039_profile_event_shards_1';
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['Shards']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04039_profile_event_shards_1'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Single distributed query: 5 shards
SELECT count() FROM remote('127.0.0.{1,2,3,4,5}', system.one) FORMAT Null SETTINGS log_comment = '04039_profile_event_shards_2';
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['Shards']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04039_profile_event_shards_2'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- 3 shards, one unavailable (non-routable address), skip_unavailable_shards = 1
-- The event should still report 3 (total expected shards, including the skipped one)
SELECT count() FROM remote('192.0.2.1,127.0.0.{1,2}', system.one) FORMAT Null SETTINGS skip_unavailable_shards = 1, connect_timeout_with_failover_ms = 100, log_comment = '04039_profile_event_shards_3';
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['Shards']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04039_profile_event_shards_3'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Two distributed sources in one query: 2 + 3 = 5 shards.
-- Scalar subqueries are evaluated on the initiator, so both `remote` calls
-- increment `Shards` locally and their values are summed into the outer query.
SELECT ignore(
    (SELECT count() FROM remote('127.0.0.{1,2}', system.one)),
    (SELECT count() FROM remote('127.0.0.{1,2,3}', system.one))
) FORMAT Null SETTINGS log_comment = '04039_profile_event_shards_4';
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['Shards']
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04039_profile_event_shards_4'
    AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;
