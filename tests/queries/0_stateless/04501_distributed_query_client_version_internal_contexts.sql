-- A distributed sub-query forwarded to a remote shard must carry a known (non-zero) initiator
-- version: the receiving server uses it for version-gated compatibility decisions. Queries
-- initiated from an internally synthesized `INITIAL_QUERY` context (a refreshable materialized
-- view refresh, or an async insert flush) used to leave the client version at 0.0.0, which now
-- makes `RemoteQueryExecutor` throw a logical error. These contexts must be filled with this
-- server's own version, because this server is the real initiator of the query and of any
-- distributed sub-query it spawns. See https://github.com/ClickHouse/ClickHouse/pull/109408

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS src_dist;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src VALUES (10), (20), (30);
CREATE TABLE src_dist (x UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), src);
CREATE TABLE dst (s UInt64) ENGINE = MergeTree ORDER BY s;

-- The refresh runs in a background context; `prefer_localhost_replica = 0` forces the read of the
-- Distributed table through `RemoteQueryExecutor` so the version guard is exercised.
CREATE MATERIALIZED VIEW mv REFRESH EVERY 1 YEAR TO dst AS
    SELECT sum(x) AS s FROM src_dist SETTINGS prefer_localhost_replica = 0;

SYSTEM REFRESH VIEW mv;
SYSTEM WAIT VIEW mv;

SELECT 'refresh', s FROM dst;

DROP TABLE IF EXISTS ins_dictsrc;
DROP TABLE IF EXISTS ins_dictsrc_dist;
DROP TABLE IF EXISTS ins_target;
DROP TABLE IF EXISTS ins_mv_dst;
DROP TABLE IF EXISTS ins_mv;

CREATE TABLE ins_dictsrc (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO ins_dictsrc VALUES (1, 100), (2, 200);
CREATE TABLE ins_dictsrc_dist (k UInt64, v UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), ins_dictsrc);
CREATE TABLE ins_target (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE ins_mv_dst (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;

-- The materialized view reads a Distributed table while the insert (and thus the view cascade) runs
-- from the async insert flush context; `prefer_localhost_replica = 0` forces the remote path.
CREATE MATERIALIZED VIEW ins_mv TO ins_mv_dst AS
    SELECT k, (SELECT sum(v) FROM ins_dictsrc_dist SETTINGS prefer_localhost_replica = 0) AS v
    FROM ins_target;

INSERT INTO ins_target SETTINGS async_insert = 1, wait_for_async_insert = 1 VALUES (1);

SELECT 'async_insert', k, v FROM ins_mv_dst;

DROP TABLE mv;
DROP TABLE dst;
DROP TABLE src_dist;
DROP TABLE src;
DROP TABLE ins_mv;
DROP TABLE ins_mv_dst;
DROP TABLE ins_target;
DROP TABLE ins_dictsrc_dist;
DROP TABLE ins_dictsrc;
