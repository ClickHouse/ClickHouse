-- A queued (asynchronous) distributed INSERT is replayed from a batch file on disk: the initiator's
-- `ClientInfo` is serialized into the batch header and later handed to `RemoteInserter` as is. When
-- the insert originates from a server-initiated query context - here a `Buffer` flush into a
-- `Distributed` table - that client info used to carry a zero version, so the receiving shard treated
-- the initiator as an ancient server and applied legacy compatibility downgrades.
-- See https://github.com/ClickHouse/ClickHouse/pull/109408

DROP TABLE IF EXISTS async_buf;
DROP TABLE IF EXISTS async_dist;
DROP TABLE IF EXISTS async_dst;

CREATE TABLE async_dst (x UInt64) ENGINE = MergeTree ORDER BY x;

-- The second shard of `test_cluster_two_shards` is not a local address, so the insert into it goes
-- over the network through `RemoteInserter` instead of being written locally. The sharding key routes
-- the single row there. Both shards point at this same server, so the row lands in `async_dst` here.
CREATE TABLE async_dist (x UInt64) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), async_dst, x);

-- Large min thresholds so the data stays in the buffer until it is flushed explicitly.
CREATE TABLE async_buf (x UInt64) ENGINE = Buffer(currentDatabase(), async_dist, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);

-- `distributed_foreground_insert` is off by default, so the flush writes a batch file that is sent
-- later: the client info makes a round trip through the on-disk batch header.
INSERT INTO async_buf VALUES (9);
OPTIMIZE TABLE async_buf;

SYSTEM FLUSH DISTRIBUTED async_dist;
SELECT 'async_insert', x FROM async_dst ORDER BY x;

SYSTEM FLUSH LOGS query_log;

-- Check the version of the replayed insert as recorded on the receiving side. It runs on the shard
-- with the cluster connection's own default database, so it is identified by the table it writes,
-- not by `current_database`. The batch header is filled with this server's own version, and the
-- shard is this same server, so the recorded tuple must match `version` exactly - a non-zero but
-- wrong version would still take wrong version-gated compatibility branches on the shard.
SELECT 'remote_version', count() > 0,
    min((client_version_major, client_version_minor, client_version_patch)
        = (toUInt64(splitByChar('.', version())[1]), toUInt64(splitByChar('.', version())[2]), toUInt64(splitByChar('.', version())[3])))
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query = 0 AND event_date >= yesterday()
    AND has(databases, currentDatabase()) AND has(tables, concat(currentDatabase(), '.async_dst'));

DROP TABLE async_buf;
DROP TABLE async_dist;
DROP TABLE async_dst;
