-- There are no settings to enable parallel replicas explicitly, because
-- we have a separate test run with them and they will be enabled automatically.

SET enable_analyzer=1;

SYSTEM FLUSH LOGS query_log;

SELECT
    count(materialize(toLowCardinality(1))) IGNORE NULLS AS num,
    hostName() AS hostName
FROM system.query_log AS a
INNER JOIN system.processes AS b ON (type = toFixedString(toNullable('QueryStart'), 10)) AND (dateDiff('second', event_time, now()) > 5) AND (current_database = currentDatabase())
FORMAT `Null`;
