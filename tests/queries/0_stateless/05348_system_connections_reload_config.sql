-- Tags: no-parallel
-- no-parallel: `SYSTEM RELOAD CONFIG` holds a server-wide lock. Many parallel copies of this test wait for each other for minutes.

-- `SYSTEM RELOAD CONFIG` reads `collect_connection_metrics` again.
-- The server config must set `collect_connection_metrics` to `true`.

-- The reload starts the listeners again. The test config has `::` and `0.0.0.0` in `listen_host`.
-- Thus, the reload logs "Address already in use" warnings. Do not send these warnings to the client.
SET send_logs_level = 'fatal';

SYSTEM RELOAD CONFIG;

SELECT value, changeable_without_restart FROM system.server_settings WHERE name = 'collect_connection_metrics';
SELECT count() > 0 FROM system.connections WHERE query_id = currentQueryID();
