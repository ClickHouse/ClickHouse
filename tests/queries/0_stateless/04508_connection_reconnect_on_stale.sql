-- Tags: no-parallel
-- - no-parallel - the fail point below affects connection establishment globally on the server

-- A pooled connection is no longer pinged before every query. A connection that the server closed
-- while it was idle in the pool is instead detected when it is first used, and is transparently
-- recovered by retrying on a freshly established connection (the connection establisher reconnects
-- once, and the failover pool retries the replica). The fail point injects one such failure during
-- connection establishment; the query must still succeed.
--
-- prefer_localhost_replica = 0 forces a real remote connection to the local server (otherwise the
-- query would run locally and never establish a connection, making the test a no-op).

SET async_socket_for_remote = 0, async_query_sending_for_remote = 0, prefer_localhost_replica = 0;

SYSTEM ENABLE FAILPOINT connection_stale_on_establish;
SELECT dummy FROM remote('127.0.0.1', system, one);
SYSTEM DISABLE FAILPOINT connection_stale_on_establish;
