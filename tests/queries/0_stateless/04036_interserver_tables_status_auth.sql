-- Tags: shard, no-fasttest
-- Regression test for interserver `TablesStatusRequest` authentication (PR #99854).
--
-- A `Distributed` query that probes remote replica status connects to the remote shard
-- in interserver-secret mode and sends a `TablesStatusRequest` during connection
-- establishment, before any query authenticates the connection. With this change the
-- request carries a cluster-secret hash that the remote validates, so a legitimate
-- `<secret>` cluster query must still succeed (the too-strict variant of this fix
-- rejected it and broke `Distributed` routing on secret clusters).
--
-- `test_cluster_interserver_secret` is defined with `<secret>` in the stateless config.

DROP TABLE IF EXISTS t_local_04036;
DROP TABLE IF EXISTS t_dist_04036;

CREATE TABLE t_local_04036 (x UInt32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_local_04036 VALUES (1);

CREATE TABLE t_dist_04036 AS t_local_04036
    ENGINE = Distributed(test_cluster_interserver_secret, currentDatabase(), t_local_04036, rand());

-- Force real interserver connections (not the local shortcut) and enable the
-- replica-staleness check that triggers `TablesStatusRequest`.
SET prefer_localhost_replica = 0;
SET max_replica_delay_for_distributed_queries = 300;
SET fallback_to_stale_replicas_for_distributed_queries = 1;

-- Both shards point at this server, so they read the same single row; DISTINCT
-- collapses it to one value and makes the result independent of the shard count.
SELECT DISTINCT x FROM t_dist_04036;

DROP TABLE t_dist_04036;
DROP TABLE t_local_04036;
