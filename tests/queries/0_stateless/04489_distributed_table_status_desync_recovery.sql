-- Tags: no-parallel, shard
-- - no-parallel: uses a fail point (global server state)
-- - shard: connects to 127.0.0.2 as a second shard

DROP TABLE IF EXISTS t_status_desync SYNC;

CREATE TABLE t_status_desync (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_status_desync SELECT number FROM numbers(10);

-- remote('127.0.0.{1,2}', ...) is two shards (each a single replica). prefer_localhost_replica = 0
-- forces a real pooled connection per shard, so the table status is actually requested over the
-- network. The fail point makes the first such `getTablesStatus` throw UNEXPECTED_PACKET_FROM_SERVER,
-- simulating a connection that a previous distributed query left out of sync in the pool (it would
-- read a stale `ProfileInfo` instead of the requested `TablesStatusResponse`). Before the fix this
-- aborted the whole distributed query; now the unusable connection is disconnected and connection
-- establishment retries on a freshly established one, so the query still returns all 20 rows.

SYSTEM ENABLE FAILPOINT unexpected_packet_in_table_status_response;
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_status_desync)
SETTINGS prefer_localhost_replica = 0, connections_with_failover_max_tries = 3, use_hedged_requests = 0;
SYSTEM DISABLE FAILPOINT unexpected_packet_in_table_status_response;

-- Same, but over the hedged-requests path (the configuration in which the original flaky failures
-- were observed).
SYSTEM ENABLE FAILPOINT unexpected_packet_in_table_status_response;
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_status_desync)
SETTINGS prefer_localhost_replica = 0, connections_with_failover_max_tries = 3, use_hedged_requests = 1;
SYSTEM DISABLE FAILPOINT unexpected_packet_in_table_status_response;

DROP TABLE t_status_desync SYNC;
