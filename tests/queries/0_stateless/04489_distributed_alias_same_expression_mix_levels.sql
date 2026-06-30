-- Tags: distributed, no-parallel, no-flaky-check

-- Deterministic regression test for the wrong-results variant of
-- https://github.com/ClickHouse/ClickHouse/issues/106403 (see also PR #107913).
-- Two ALIAS columns expanding to the same expression (a1, a2 both = toString(x)) are collapsed into a single GROUP BY
-- key on the shard before it computes two-level bucket numbers, while the initiator keeps them distinct. The bug only
-- manifests when one shard converts to two-level and another stays single-level, so equal groups arriving from the two
-- shards in different bucketing schemes are never merged. That single-level/two-level mix needs genuinely asymmetric
-- per-shard data, which test_cluster_two_shards_different_databases provides (shard_0 and shard_1 are distinct local
-- databases on the same server). shard_0 holds 1000 groups (over the two-level threshold), shard_1 holds 10 groups
-- 0..9 (single-level). Pre-fix the 10 overlapping groups came back twice with count 1 (1010 rows, 0 merged); the fix
-- merges them into one row with count 2.

DROP DATABASE IF EXISTS shard_0;
DROP DATABASE IF EXISTS shard_1;
CREATE DATABASE shard_0;
CREATE DATABASE shard_1;

CREATE TABLE shard_0.t_04489 (dt DateTime, x UInt64, a1 String ALIAS toString(x), a2 String ALIAS toString(x)) ENGINE = MergeTree ORDER BY dt;
CREATE TABLE shard_1.t_04489 (dt DateTime, x UInt64, a1 String ALIAS toString(x), a2 String ALIAS toString(x)) ENGINE = MergeTree ORDER BY dt;

INSERT INTO shard_0.t_04489 (dt, x) SELECT '2024-01-01 00:00:00', number FROM numbers(1000);
INSERT INTO shard_1.t_04489 (dt, x) SELECT '2024-01-01 00:00:00', number FROM numbers(10);

CREATE TABLE dist_04489 (dt DateTime, x UInt64, a1 String ALIAS toString(x), a2 String ALIAS toString(x))
ENGINE = Distributed('test_cluster_two_shards_different_databases', '', 't_04489');

SET enable_analyzer = 1;

-- Order-independent summary: 1000 distinct groups total, the 10 overlapping ones (0..9) merged to count 2, the rest
-- count 1, so sum(count()) = 1010. Pre-fix this was 1010 rows / 0 merged / 1010 total (every group split per shard).
SELECT count() AS groups, countIf(c = 2) AS merged_groups, sum(c) AS total_rows
FROM (SELECT a1, a2, count() AS c FROM dist_04489 GROUP BY a1, a2)
SETTINGS group_by_two_level_threshold = 50, group_by_two_level_threshold_bytes = 0, prefer_localhost_replica = 0, distributed_aggregation_memory_efficient = 1;

SELECT count() AS groups, countIf(c = 2) AS merged_groups, sum(c) AS total_rows
FROM (SELECT a1, a2, count() AS c FROM dist_04489 GROUP BY a1, a2)
SETTINGS group_by_two_level_threshold = 50, group_by_two_level_threshold_bytes = 0, prefer_localhost_replica = 0, distributed_aggregation_memory_efficient = 0;

-- The overlapping groups, listed explicitly: each must appear exactly once with count 2 (pre-fix this result was empty).
SELECT a1, a2, count() AS c FROM dist_04489 GROUP BY a1, a2 HAVING c > 1 ORDER BY toUInt64(a1)
SETTINGS group_by_two_level_threshold = 50, group_by_two_level_threshold_bytes = 0, prefer_localhost_replica = 0, distributed_aggregation_memory_efficient = 1;

DROP TABLE dist_04489;
DROP DATABASE shard_0;
DROP DATABASE shard_1;
