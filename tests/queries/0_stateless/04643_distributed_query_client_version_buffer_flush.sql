-- The flush of a `Buffer` table runs in an internally synthesized query context
-- (`StorageBuffer::writeBlockToDestination` creates it from the table's global context), so it
-- must be filled with this server's own version: if the destination table has a materialized view
-- that reads from a `Distributed` table, the flush spawns a distributed sub-query, and
-- `RemoteQueryExecutor` throws a logical error on a zero client version.
-- See https://github.com/ClickHouse/ClickHouse/pull/109408

DROP TABLE IF EXISTS buf;
DROP TABLE IF EXISTS buf_mv;
DROP TABLE IF EXISTS buf_mv_dst;
DROP TABLE IF EXISTS buf_dst;
DROP TABLE IF EXISTS agg_src_dist;
DROP TABLE IF EXISTS agg_src;

CREATE TABLE agg_src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO agg_src VALUES (7), (8);
CREATE TABLE agg_src_dist (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), agg_src);

CREATE TABLE buf_dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE buf_mv_dst (x UInt64, s UInt64) ENGINE = MergeTree ORDER BY x;

-- The view fires during the buffer flush; `prefer_localhost_replica = 0` forces the read of the
-- Distributed table through `RemoteQueryExecutor` so the version guard is exercised.
CREATE MATERIALIZED VIEW buf_mv TO buf_mv_dst AS
    SELECT x, (SELECT sum(x) FROM agg_src_dist SETTINGS prefer_localhost_replica = 0) AS s
    FROM buf_dst;

-- Large min thresholds so the data stays in the buffer until it is flushed explicitly.
CREATE TABLE buf (x UInt64) ENGINE = Buffer(currentDatabase(), buf_dst, 1, 1000, 1000, 1000000, 1000000, 100000000, 100000000);

INSERT INTO buf VALUES (5);
OPTIMIZE TABLE buf;

SELECT 'buffer_flush', x, s FROM buf_mv_dst;

-- The invariant is about what the *receiving* server observes: it gates compatibility decisions
-- (e.g. disabling the analyzer for a pre-23.3 initiator, `TCPHandler::receiveQuery`) on the
-- `client_version_*` it reads from the forwarded `ClientInfo`. A synthesized server-side context
-- keeps the default `ClientInfo::Interface::TCP`, which is exactly the interface for which
-- `ClientInfo::write` serializes the version, so a zero version is what the shard used to see.
-- Check the version of the forwarded sub-query as recorded on the receiving side. The synthesized
-- context is filled with this server's own version, and the shard is this same server, so the
-- recorded tuple must match `version` exactly - a non-zero but wrong version would still take
-- wrong version-gated compatibility branches on the shard.
SYSTEM FLUSH LOGS query_log;

SELECT 'remote_version', count() > 0,
    min((client_version_major, client_version_minor, client_version_patch)
        = (toUInt64(splitByChar('.', version())[1]), toUInt64(splitByChar('.', version())[2]), toUInt64(splitByChar('.', version())[3])))
FROM system.query_log
-- The sub-query runs on the shard with its connection's own default database, so it is identified
-- by the table it reads, not by `current_database`. With `serialize_query_plan = 1` the shard runs
-- a deserialized query plan and never analyses the query, so it records no `databases`/`tables`
-- either - hence the identification by the query text, which is qualified with this test's database
-- in both cases.
WHERE type = 'QueryFinish' AND is_initial_query = 0 AND event_date >= yesterday()
    AND (query LIKE concat('%`', currentDatabase(), '`.`agg_src`%')
        OR has(databases, currentDatabase()));

DROP TABLE buf;
DROP TABLE buf_mv;
DROP TABLE buf_mv_dst;
DROP TABLE buf_dst;
DROP TABLE agg_src_dist;
DROP TABLE agg_src;
