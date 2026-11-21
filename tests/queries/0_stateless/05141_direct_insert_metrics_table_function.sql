-- Pins the documented contract of DirectInsertedRows/DirectInsertedBytes for INSERT queries
-- whose immediate destination is not a table: writes into the sink of a table function are
-- attributed to the "Direct" counters as well, and never to the "MaterializedView" counters.
-- Per-query ProfileEvents from system.query_log (isolated by current_database) make the check
-- deterministic. The INSERT query is tagged with an inline comment so it can be found in the log.

SET log_queries = 1;
SET log_queries_min_type = 'QUERY_FINISH';

INSERT INTO /* test 05141 */ FUNCTION null('id UInt64, s String') SELECT number, toString(number) FROM numbers(7);

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['DirectInsertedRows'],
    ProfileEvents['MaterializedViewInsertedRows'],
    ProfileEvents['DirectInsertedBytes'] > 0,
    ProfileEvents['DirectInsertedRows'] + ProfileEvents['MaterializedViewInsertedRows'] = ProfileEvents['InsertedRows'],
    ProfileEvents['DirectInsertedBytes'] + ProfileEvents['MaterializedViewInsertedBytes'] = ProfileEvents['InsertedBytes']
FROM system.query_log
WHERE current_database = currentDatabase()
  AND query LIKE 'INSERT INTO /* test 05141 */%'
  AND type = 'QueryFinish'
  AND event_date >= yesterday()
ORDER BY event_time DESC
LIMIT 1;
