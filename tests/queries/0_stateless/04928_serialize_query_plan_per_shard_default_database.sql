-- https://github.com/ClickHouse/ClickHouse/issues/111893
-- A Distributed table declared with an empty database (relying on a per-shard
-- <default_database> to fill it in at query time) used to fail under
-- serialize_query_plan=1 -- the query tree built for shipping to a shard carried the
-- empty database placeholder from before any per-shard information was available, and
-- the fallback resolution path that used to fill it in (via
-- Context::setCurrentDatabase + fresh catalog lookup) was bypassed once the plan
-- serialization path started reusing an already-resolved query tree instead.

DROP TABLE IF EXISTS shard_0.t_04928;
DROP TABLE IF EXISTS shard_1.t_04928;
DROP TABLE IF EXISTS d_04928;
DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;

CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

CREATE TABLE shard_0.t_04928 (k UInt32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE shard_1.t_04928 (k UInt32) ENGINE = MergeTree ORDER BY k;

INSERT INTO shard_0.t_04928 SELECT number FROM numbers(5);
INSERT INTO shard_1.t_04928 SELECT number + 100 FROM numbers(5);

-- Empty database string: the Distributed table relies entirely on each shard's own
-- <default_database> (see test_cluster_two_shards_different_databases in the stock
-- test config) to resolve which table to read.
CREATE TABLE d_04928 AS shard_0.t_04928
    ENGINE = Distributed('test_cluster_two_shards_different_databases', '', 't_04928');

-- was: Code 60/81, UNKNOWN_TABLE/UNKNOWN_DATABASE -- must now return the same 10 rows
-- as serialize_query_plan=0.
SELECT k FROM d_04928 ORDER BY k SETTINGS serialize_query_plan = 0;
SELECT k FROM d_04928 ORDER BY k SETTINGS serialize_query_plan = 1;

DROP TABLE d_04928;
DROP TABLE shard_0.t_04928;
DROP TABLE shard_1.t_04928;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
