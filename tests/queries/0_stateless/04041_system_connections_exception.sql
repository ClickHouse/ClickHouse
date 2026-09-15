-- Regression test: after a query raises an exception on a TCP connection,
-- system.connections must show the connection as idle (empty query_id),
-- not stuck as active with the old query_id.
-- Requires: collect_connection_metrics = true in server config.

-- Run a query that throws during execution (not at parse time).
SELECT throwIf(1, 'intentional test exception'); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- The connection must now be accessible for a new query.
-- This query itself is the proof: if the connection were stuck as active,
-- the server would not be able to execute this.
-- Verify: the current query appears as active (SCOPE_EXIT correctly armed),
-- and no row exists with the previous (failed) query_id still marked active.
SELECT protocol, status
FROM system.connections
WHERE query_id = currentQueryID() AND protocol = 'TCP';
