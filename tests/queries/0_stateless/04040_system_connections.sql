-- Schema and basic behavior of system.connections, with a TCP connection.
-- The server config must set collect_connection_metrics to true.

-- 1. Make sure that system.tables contains the table.
SELECT count() > 0 FROM system.tables WHERE database = 'system' AND name = 'connections';

-- 2. Make sure that the column names and types do not change.
SELECT name, type
FROM system.columns
WHERE database = 'system' AND table = 'connections'
ORDER BY position;

-- 3. The current TCP connection must be 'active' and have the current query_id.
--    Find the row where query_id is equal to currentQueryID().
SELECT protocol, status
FROM system.connections
WHERE query_id = currentQueryID() AND protocol = 'TCP';

-- 4. All connections must have a user and a server_port that is not 0.
SELECT count() FROM system.connections WHERE user = '' OR server_port = 0;
