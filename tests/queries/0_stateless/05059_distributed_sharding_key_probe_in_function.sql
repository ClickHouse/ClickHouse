-- A `Merge` table asks every child for a processing stage. A child column that does not convert to
-- the `Merge` header type order-preservingly forces that stage down to `FetchColumns`, and only at
-- that stage does a `Distributed` child evaluate its sharding-key predicate against the planner
-- context that `ReadFromMerge` derives per child. A derived planner context owns no registered sets.
SET explain_query_plan_default = 'legacy';
SET allow_experimental_analyzer = 1;
SET optimize_skip_unused_shards = 1;
SET optimize_distributed_group_by_sharding_key = 1;
SET prefer_localhost_replica = 0;

DROP TABLE IF EXISTS t_probe_u;
DROP TABLE IF EXISTS t_probe_i;
DROP TABLE IF EXISTS t_probe_dist_u;
DROP TABLE IF EXISTS t_probe_dist_i;

CREATE TABLE t_probe_u (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_probe_i (k UInt64, v Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_probe_u SELECT number, number FROM numbers(4);
INSERT INTO t_probe_i SELECT number, number FROM numbers(4);
CREATE TABLE t_probe_dist_u AS t_probe_u
    ENGINE = Distributed(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), t_probe_u, k);
CREATE TABLE t_probe_dist_i AS t_probe_i
    ENGINE = Distributed(test_cluster_two_shard_three_replicas_localhost, currentDatabase(), t_probe_i, k);

-- `v` is the column whose conversion into the header type is not order-preserving, so it is what
-- puts the queries below on the `FetchColumns` path. Pinned, so that a change to the type
-- unification reddens here rather than leaving the cases below exercising nothing.
DESCRIBE TABLE merge(currentDatabase(), '^t_probe_dist_[ui]$');

-- Every `IN`-family second-argument kind the sharding-key predicate can meet, at each of the three
-- clauses it is evaluated for: `GROUP BY`, `DISTINCT` and `LIMIT BY`.
SELECT count() FROM merge(currentDatabase(), '^t_probe_dist_[ui]$') GROUP BY k IN (1, 2) ORDER BY 1;
SELECT count(NULL) FROM merge(currentDatabase(), '^t_probe_dist_[ui]$') GROUP BY k GLOBAL IN (NULL) ORDER BY 1;
SELECT count() FROM merge(currentDatabase(), '^t_probe_dist_[ui]$') GROUP BY k IN (SELECT number FROM numbers(2)) ORDER BY 1;
SELECT count() FROM merge(currentDatabase(), '^t_probe_dist_[ui]$') GROUP BY k IN (SELECT 1 UNION ALL SELECT 2) ORDER BY 1;
SELECT DISTINCT k IN (1, 2) FROM merge(currentDatabase(), '^t_probe_dist_[ui]$') ORDER BY 1;
SELECT k FROM merge(currentDatabase(), '^t_probe_dist_[ui]$') ORDER BY k LIMIT 1 BY k IN (1, 2);

-- The predicate still accepts a plain sharding-key `GROUP BY`, so the shards keep aggregating on
-- their own: `ReadFromRemote` alone, with no `MergingAggregated` on the initiator.
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_probe_dist_u GROUP BY k) WHERE explain ILIKE '%ReadFromRemote%';
SELECT count() FROM (EXPLAIN SELECT count() FROM t_probe_dist_u GROUP BY k) WHERE explain ILIKE '%MergingAggregated%';

DROP TABLE t_probe_dist_u;
DROP TABLE t_probe_dist_i;
DROP TABLE t_probe_u;
DROP TABLE t_probe_i;
