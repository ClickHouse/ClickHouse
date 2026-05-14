-- Tags: shard
--
-- Regression test for AST fuzzer crash in `OptimizeShardingKeyRewriteIn`:
-- an empty constant tuple in an `IN ()` against a `Distributed` table
-- whose sharding key matches the IN column triggered `Tuple::back()` on
-- an empty vector. The query is degenerate (`IN ()` is always false),
-- but it must not crash the server.

DROP TABLE IF EXISTS dist_04238;

CREATE TABLE dist_04238 AS system.one ENGINE = Distributed(test_cluster_two_shards, system, one, intHash64(dummy));

SET prefer_localhost_replica = 0;
SET optimize_skip_unused_shards = 1;
SET optimize_skip_unused_shards_rewrite_in = 1;

SELECT * FROM dist_04238 WHERE dummy IN tuple() ORDER BY dummy FORMAT Null;

SELECT (dummy IN tuple()) FROM dist_04238 ORDER BY dummy LIMIT 1 FORMAT Null;

DROP TABLE dist_04238;
