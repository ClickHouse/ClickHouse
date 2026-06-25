-- Regression test for PR #104948: `defaultValueOfTypeName` resolves the user-supplied
-- type name via its own `getReturnTypeImpl` (outside `SuppressQueryFactoriesInfoScope`),
-- so the type family must be attributed to `query_log.used_data_type_families`.

SET log_queries = 1;

SELECT defaultValueOfTypeName('IPv4') FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT has(used_data_type_families, 'IPv4')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND query LIKE '%defaultValueOfTypeName%'
  AND query NOT LIKE '%system.query_log%'
ORDER BY query_start_time DESC
LIMIT 1;
