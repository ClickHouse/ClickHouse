-- `SYSTEM RELOAD CONFIG` reads `collect_connection_metrics` again.
-- The server config must set `collect_connection_metrics` to `true`.

SYSTEM RELOAD CONFIG;

SELECT value, changeable_without_restart FROM system.server_settings WHERE name = 'collect_connection_metrics';
SELECT count() > 0 FROM system.connections WHERE query_id = currentQueryID();
