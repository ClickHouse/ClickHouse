-- Regression test for PR #104948: the function resolves the user-supplied type name via its
-- own getReturnTypeImpl (outside SuppressQueryFactoriesInfoScope), so the type family must be
-- attributed to query_log.used_data_type_families. The query is tagged with a unique
-- log_comment so the filter cannot accidentally match the preceding statements.

SET log_queries = 1;

SELECT defaultValueOfTypeName('IPv4') SETTINGS log_comment = '04411_dvotn_query_log_marker' FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT has(used_data_type_families, 'IPv4')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04411_dvotn_query_log_marker'
ORDER BY query_start_time DESC
LIMIT 1;
