-- `bitmapBuild` has the declarative signature `(Array(T : Integer)) -> AggregateFunction('groupBitmap', T)`,
-- so computing its return type resolves the `groupBitmap` aggregate through `AggregateFunctionFactory`.
-- That name is a signature internal, not an aggregate the user invoked, so it must not leak into
-- `query_log.used_aggregate_functions`.
--
-- `materialize` keeps the argument non-constant so `bitmapBuild` is not constant-folded (executing it
-- would resolve `groupBitmap` at execution time — a separate, pre-existing path); `toTypeName` needs only
-- the return type, so this isolates the signature/return-type analysis path.

SET log_queries = 1;

SELECT toTypeName(bitmapBuild(materialize([1, 2, 3, 4, 5]))) FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT has(used_aggregate_functions, 'groupBitmap')
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - INTERVAL 600 SECOND
  AND current_database = currentDatabase() AND type = 'QueryFinish'
  AND query LIKE '%toTypeName(bitmapBuild(materialize%' AND query NOT LIKE '%system.query_log%'
ORDER BY event_time DESC LIMIT 1;
