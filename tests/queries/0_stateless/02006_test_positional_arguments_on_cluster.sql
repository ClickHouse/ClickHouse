-- Tags: no-ordinary-database, no-replicated-database, distributed, zookeeper

DROP TABLE IF EXISTS t02006 on cluster test_shard_localhost format Null;
DROP TABLE IF EXISTS m02006 on cluster test_shard_localhost format Null;

CREATE TABLE t02006 on cluster test_shard_localhost (d Date) 
ENGINE = MergeTree ORDER BY d
format Null;

CREATE MATERIALIZED VIEW m02006 on cluster test_shard_localhost
Engine = MergeTree ORDER BY tuple() AS SELECT d, 0 AS i FROM t02006 GROUP BY d, i
format Null;

ALTER TABLE t02006 on cluster test_shard_localhost ADD COLUMN IF NOT EXISTS f UInt64 format Null;

DESC t02006;

DROP TABLE IF EXISTS t02006 on cluster test_shard_localhost format Null;
DROP TABLE IF EXISTS m02006 on cluster test_shard_localhost format Null;
