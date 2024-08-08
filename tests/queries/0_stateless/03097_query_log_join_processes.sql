-- https://github.com/ClickHouse/ClickHouse/issues/56521

SYSTEM FLUSH LOGS;

SET enable_analyzer=1;

SELECT count(1) as num, hostName() as hostName FROM system.query_log as a INNER JOIN system.processes as b on a.query_id = b.query_id and type = 'QueryStart' and dateDiff('second', event_time, now()) > 5 and current_database = currentDatabase() FORMAT Null;
