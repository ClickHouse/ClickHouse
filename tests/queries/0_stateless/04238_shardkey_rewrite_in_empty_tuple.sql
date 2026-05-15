-- Tags: shard
--
-- Regression test for the AST-fuzzer-found crash in `OptimizeShardingKeyRewriteIn`:
-- when the analyzer-side rewriter visited an `IN tuple()` (empty constant tuple) against
-- the sharding key column of a `Distributed` table, it called `Tuple::back()` on an
-- empty `std::vector`, aborting under libc++ hardening with
-- "back() called on an empty vector".
--
-- The trigger needs the rewriter to actually run on the empty tuple, which requires
-- `optimize_skip_unused_shards` to populate `optimized_cluster` (so `shards > 1`).
-- An empty `IN ()` on the sharding key column in WHERE fully prunes all shards and
-- bypasses the rewriter, so the test combines:
--   * a non-empty WHERE IN on the sharding key (keeps shards alive), and
--   * an empty `tuple()` IN elsewhere (visited by the rewriter and previously crashing).

DROP TABLE IF EXISTS dist_04238;

CREATE TABLE dist_04238 AS system.one ENGINE = Distributed(test_cluster_two_shards, system, one, intHash64(dummy));

SET prefer_localhost_replica = 0;
SET optimize_skip_unused_shards = 1;
SET optimize_skip_unused_shards_rewrite_in = 1;

SELECT count() FROM dist_04238
WHERE dummy IN (0, 2)
GROUP BY (dummy IN tuple())
FORMAT Null;

DROP TABLE dist_04238;
