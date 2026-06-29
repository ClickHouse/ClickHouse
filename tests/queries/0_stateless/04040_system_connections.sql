-- system.connections: schema and basic behavior via TCP
-- Requires: collect_connection_metrics = true in server config.

-- 1. Verify that the table is listed in system.tables.
SELECT count() > 0 FROM system.tables WHERE database = 'system' AND name = 'connections';

-- 2. Verify column names and types are stable.
SELECT name, type
FROM system.columns
WHERE database = 'system' AND table = 'connections'
ORDER BY position;

-- 3. The current TCP connection must appear as 'active' with the current query_id.
--    We look for a row where query_id matches currentQueryID().
SELECT protocol, status
FROM system.connections
WHERE query_id = currentQueryID() AND protocol = 'TCP';

-- 4. Non-empty user and non-zero server_port for all connections.
SELECT count() FROM system.connections WHERE user = '' OR server_port = 0;
