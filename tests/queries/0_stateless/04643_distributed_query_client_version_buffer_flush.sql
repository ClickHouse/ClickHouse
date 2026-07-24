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

DROP TABLE buf;
DROP TABLE buf_mv;
DROP TABLE buf_mv_dst;
DROP TABLE buf_dst;
DROP TABLE agg_src_dist;
DROP TABLE agg_src;
