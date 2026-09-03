-- Reading from a `Merge` table with plan-based parallel replicas. `ReadFromMerge` is opaque to the
-- parallel-replicas plan transformation (it unites the pipelines of its per-table subplans, not their
-- plans), so it is first expanded into a plan-level union of the underlying `ReadFromMergeTree` reads.
-- The union is then distributed like any other union: the reads are coordinated across the replicas and
-- the aggregation on top of the `Merge` is split into a partial aggregation on the replicas and a
-- `MergingAggregated` on the initiator. Gated by `parallel_replicas_allow_merge_tables`.

DROP TABLE IF EXISTS t_pr_merge_1;
DROP TABLE IF EXISTS t_pr_merge_2;
DROP TABLE IF EXISTS m_pr_merge;
DROP TABLE IF EXISTS t_pr_mergemix_mt;
DROP TABLE IF EXISTS t_pr_mergemix_memory;
DROP TABLE IF EXISTS m_pr_mergemix;
DROP TABLE IF EXISTS t_pr_mergefinal_1;
DROP TABLE IF EXISTS t_pr_mergefinal_2;
DROP TABLE IF EXISTS m_pr_mergefinal;

CREATE TABLE t_pr_merge_1 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_pr_merge_2 (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_pr_merge_1 SELECT number, number % 10 FROM numbers(50000);
INSERT INTO t_pr_merge_2 SELECT number + 50000, number % 10 FROM numbers(50000);
CREATE TABLE m_pr_merge (a UInt64, b UInt64) ENGINE = Merge(currentDatabase(), '^t_pr_merge_[12]$');

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
-- Pin the manual mode: otherwise CI's randomized automatic_parallel_replicas_mode can cost-decide
-- against parallel replicas for this small table, so the plan-based split does not engage.
SET automatic_parallel_replicas_mode = 0;
SET parallel_replicas_allow_merge_tables = 1;

-- Slow down the initiator's local read so the remote replicas emit rows before the local read completes.
-- Without coordination this is what makes the "every replica reads everything" (N x) bug deterministic.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- Correctness: identical to non-parallel execution, and the counts are not multiplied across replicas.
SELECT count(), sum(a), min(a), max(a) FROM m_pr_merge;
SELECT count(), sum(a) FROM m_pr_merge WHERE a > 60000;
SELECT b, count() FROM m_pr_merge GROUP BY b ORDER BY b;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- The same results without parallel replicas.
SELECT count(), sum(a), min(a), max(a) FROM m_pr_merge SETTINGS enable_parallel_replicas = 0;
SELECT count(), sum(a) FROM m_pr_merge WHERE a > 60000 SETTINGS enable_parallel_replicas = 0;
SELECT b, count() FROM m_pr_merge GROUP BY b ORDER BY b SETTINGS enable_parallel_replicas = 0;

-- Plan shape: the `Merge` is expanded into a union of the underlying reads, the reads are distributed and
-- the aggregation above the `Merge` is split (partial aggregation shipped, `MergingAggregated` on top).
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%ReadFromMerge%' AND explain NOT LIKE '%ReadFromMergeTree%') > 0 AS has_merge_read,
    countIf(explain LIKE '%MergingAggregated%') > 0 AS has_merging_aggregated
FROM (EXPLAIN pretty = 0, description = 0 SELECT b, count() FROM m_pr_merge GROUP BY b);

-- Without the gate the `Merge` is read by a single replica: no expansion, no distribution.
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%ReadFromMerge%' AND explain NOT LIKE '%ReadFromMergeTree%') > 0 AS has_merge_read
FROM (EXPLAIN pretty = 0, description = 0 SELECT b, count() FROM m_pr_merge GROUP BY b)
SETTINGS parallel_replicas_allow_merge_tables = 0;

-- A `Merge` over a table which is not a `MergeTree` cannot be coordinated at the level of marks: no
-- expansion, correct results.
CREATE TABLE t_pr_mergemix_mt (a UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE t_pr_mergemix_memory (a UInt64) ENGINE = Memory;
INSERT INTO t_pr_mergemix_mt SELECT number FROM numbers(1000);
INSERT INTO t_pr_mergemix_memory SELECT number FROM numbers(1000);
CREATE TABLE m_pr_mergemix (a UInt64) ENGINE = Merge(currentDatabase(), '^t_pr_mergemix_');

SELECT count(), sum(a) FROM m_pr_mergemix;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN pretty = 0, description = 0 SELECT count() FROM m_pr_mergemix);

-- `FINAL` is incompatible with parallel reading, so a `Merge` read with `FINAL` stays on a single replica.
CREATE TABLE t_pr_mergefinal_1 (a UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY a;
CREATE TABLE t_pr_mergefinal_2 (a UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY a;
INSERT INTO t_pr_mergefinal_1 SELECT number % 500, number FROM numbers(1000);
INSERT INTO t_pr_mergefinal_2 SELECT number % 500, number FROM numbers(1000);
CREATE TABLE m_pr_mergefinal (a UInt64, v UInt64) ENGINE = Merge(currentDatabase(), '^t_pr_mergefinal_[12]$');

SELECT count(), sum(a) FROM m_pr_mergefinal FINAL;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN pretty = 0, description = 0 SELECT count() FROM m_pr_mergefinal FINAL);

DROP TABLE m_pr_mergefinal;
DROP TABLE t_pr_mergefinal_2;
DROP TABLE t_pr_mergefinal_1;
DROP TABLE m_pr_mergemix;
DROP TABLE t_pr_mergemix_memory;
DROP TABLE t_pr_mergemix_mt;
DROP TABLE m_pr_merge;
DROP TABLE t_pr_merge_2;
DROP TABLE t_pr_merge_1;
