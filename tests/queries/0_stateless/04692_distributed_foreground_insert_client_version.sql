-- A foreground (synchronous) distributed INSERT does not go through the on-disk batch header: the
-- sink hands `context->getClientInfo()` straight to `RemoteInserter`, which forwards it to the shard.
-- This is the sibling of the queued path covered by
-- `04654_distributed_async_insert_client_version`, and it must carry a non-zero initiator version
-- as well, otherwise the receiving shard treats the initiator as an ancient server and applies
-- legacy compatibility downgrades.
-- See https://github.com/ClickHouse/ClickHouse/pull/109408

DROP TABLE IF EXISTS fg_src;
DROP TABLE IF EXISTS fg_mv;
DROP TABLE IF EXISTS fg_dist;
DROP TABLE IF EXISTS fg_dst;

CREATE TABLE fg_dst (x UInt64) ENGINE = MergeTree ORDER BY x;

-- The second shard of `test_cluster_two_shards` is not a local address, so the insert into it goes
-- over the network through `RemoteInserter` instead of being written locally. Both shards point at
-- this same server, so the rows land in `fg_dst` here.
CREATE TABLE fg_dist (x UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), fg_dst, x);

-- A materialized view is used so that the push into the `Distributed` table is not the top-level
-- query itself: the sink runs with the client info of the context that fires the view.
CREATE TABLE fg_src (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW fg_mv TO fg_dist AS SELECT x FROM fg_src;

-- `async_insert` makes the push run from the flush context synthesized by `AsynchronousInsertQueue`
-- instead of from the client's own query context, so the version the shard sees comes from a
-- server-created context - exactly the case this PR fills in.
SET async_insert = 1;
SET distributed_foreground_insert = 1;
INSERT INTO fg_src VALUES (11);

SELECT 'foreground_insert', x FROM fg_dst ORDER BY x;

-- A `remote()` table function has no persistent data path, so its insert is always synchronous and
-- always takes the `RemoteInserter` path, regardless of `distributed_foreground_insert`.
INSERT INTO FUNCTION remote('127.0.0.2', currentDatabase(), fg_dst) VALUES (12);

SELECT 'remote_function_insert', x FROM fg_dst ORDER BY x;

SYSTEM FLUSH LOGS query_log;

-- Check the version of the forwarded inserts as recorded on the receiving side. They run on the
-- shard with the cluster connection's own default database, so they are identified by the table
-- they write, not by `current_database`. The synthesized flush context is filled with this
-- server's own version, and the shard is this same server, so the recorded tuple must match
-- `version` exactly - a non-zero but wrong version would still take wrong version-gated
-- compatibility branches on the shard.
SELECT 'remote_version', count() > 0,
    min((client_version_major, client_version_minor, client_version_patch)
        = (toUInt64(splitByChar('.', version())[1]), toUInt64(splitByChar('.', version())[2]), toUInt64(splitByChar('.', version())[3])))
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query = 0 AND event_date >= yesterday()
    AND has(databases, currentDatabase()) AND has(tables, concat(currentDatabase(), '.fg_dst'));

DROP TABLE fg_src;
DROP TABLE fg_mv;
DROP TABLE fg_dist;
DROP TABLE fg_dst;
