-- The query shapes a `Merge` table can be read with, under plan-based parallel replicas: the `merge` table
-- function, a filter on the `_table` virtual column, a query without aggregation, a query reading in order
-- with a limit, and the shapes which are not expanded and stay on a single replica (`FINAL`, a child which
-- is not a `MergeTree` table, no matching table at all). Every result must be the one of a non-parallel
-- execution, and the distribution is asserted through the plan: a `ReadFromParallelReplicas` read
-- together with a `MergingAggregated` step means the reads are distributed and the partial aggregation
-- runs on the replicas; neither of them means the query is executed on a single replica.

DROP TABLE IF EXISTS t_pbm_1;
DROP TABLE IF EXISTS t_pbm_2;
DROP TABLE IF EXISTS t_pbm_log;
DROP TABLE IF EXISTS m_pbm;
DROP TABLE IF EXISTS m_pbm_over_log;
DROP TABLE IF EXISTS m_pbm_over_nothing;
DROP TABLE IF EXISTS t_pbm_base;
DROP VIEW IF EXISTS t_pbm_view;
DROP TABLE IF EXISTS m_pbm_over_view;

CREATE TABLE t_pbm_1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_pbm_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
INSERT INTO t_pbm_1 SELECT number, number * 2 FROM numbers(1000);
INSERT INTO t_pbm_2 SELECT number + 1000, number FROM numbers(1000);

CREATE TABLE m_pbm ENGINE = Merge(currentDatabase(), '^t_pbm_[12]$');

SELECT '-- non-parallel';
SELECT count(), sum(k), sum(v) FROM m_pbm;

SET enable_analyzer = 1;
SET max_threads = 4;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_local_plan = 1;
-- Pin the manual mode: otherwise CI's randomized automatic_parallel_replicas_mode can cost-decide
-- against parallel replicas for this small table, so the plan-based split does not engage.
SET automatic_parallel_replicas_mode = 0;

-- Setting disabled: the `Merge` read is not expanded and nothing is distributed.
SELECT '-- parallel_replicas_allow_merge_tables = 0';
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%MergingAggregated%') > 0 AS has_merging_aggregated
FROM (EXPLAIN pretty = 0, description = 0 SELECT count(), sum(k), sum(v) FROM m_pbm SETTINGS parallel_replicas_allow_merge_tables = 0);
SELECT count(), sum(k), sum(v) FROM m_pbm SETTINGS parallel_replicas_allow_merge_tables = 0;

SET parallel_replicas_allow_merge_tables = 1;

-- Setting enabled: the aggregation is distributed, the initiator merges the partial aggregation states.
SELECT '-- parallel_replicas_allow_merge_tables = 1';
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%MergingAggregated%') > 0 AS has_merging_aggregated
FROM (EXPLAIN pretty = 0, description = 0 SELECT count(), sum(k), sum(v) FROM m_pbm);

-- Slow down the initiator's local read so that the remote replicas actually produce rows: rows read both
-- locally and remotely would then surface as wrong aggregates.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT count(), sum(k), sum(v) FROM m_pbm;

SELECT '-- merge() table function';
SELECT count(), sum(k), sum(v) FROM merge(currentDatabase(), '^t_pbm_[12]$');
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN pretty = 0, description = 0 SELECT count(), sum(k), sum(v) FROM merge(currentDatabase(), '^t_pbm_[12]$'));

-- The children the query reads are pruned by the pushed-down filter on the virtual column, and the
-- expansion runs after the push-down, so only the selected child is read - and it is still distributed.
SELECT '-- filter on the _table virtual column';
SELECT count(), sum(k), sum(v) FROM m_pbm WHERE _table = 't_pbm_2';
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN pretty = 0, description = 0 SELECT count(), sum(k), sum(v) FROM m_pbm WHERE _table = 't_pbm_2');

SELECT '-- plain SELECT without aggregation';
SELECT k, v FROM m_pbm WHERE k IN (1, 999, 1000, 1999) ORDER BY k;

-- Reading in order with a limit makes the `Merge` table materialize the plans of its underlying tables,
-- and so does the expansion; the two must not clash over the same plans.
SELECT '-- ORDER BY with LIMIT';
SELECT k, v FROM m_pbm ORDER BY k LIMIT 5;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN pretty = 0, description = 0 SELECT k, v FROM m_pbm ORDER BY k LIMIT 5);

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- `FINAL` is incompatible with parallel reading: the query must stay on a single replica instead of
-- failing, both for the `Merge` table and for the `merge` table function.
SELECT '-- FINAL over a Merge table';
SELECT count(), sum(k), sum(v) FROM m_pbm FINAL;
SELECT countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read
FROM (EXPLAIN pretty = 0, description = 0 SELECT count(), sum(k), sum(v) FROM m_pbm FINAL);
SELECT '-- FINAL over the merge() table function';
SELECT count(), sum(k), sum(v) FROM merge(currentDatabase(), '^t_pbm_[12]$') FINAL;

-- A table which is not a `MergeTree` has no marks to coordinate, so the `Merge` read is not expanded.
SELECT '-- Merge over a non-MergeTree table';
CREATE TABLE t_pbm_log (k UInt64, v UInt64) ENGINE = Log;
INSERT INTO t_pbm_log SELECT number + 2000, number FROM numbers(100);
CREATE TABLE m_pbm_over_log ENGINE = Merge(currentDatabase(), '^t_pbm_(1|2|log)$');
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%MergingAggregated%') > 0 AS has_merging_aggregated
FROM (EXPLAIN pretty = 0, description = 0 SELECT count(), sum(k), sum(v) FROM m_pbm_over_log);
SELECT count(), sum(k), sum(v) FROM m_pbm_over_log;

-- A child read through an interpreter is not expanded either, even though the plan of a `View` over a
-- single `MergeTree` table has the very same shape as the plan of a plain `MergeTree` child: such a child
-- is planned with parallel replicas cleared from its context, so its read could not be distributed anyway.
-- The `Merge` read must be left as it is instead of being expanded for nothing.
SELECT '-- Merge over a View';
CREATE TABLE t_pbm_base (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pbm_base SELECT number + 3000, number FROM numbers(100);
CREATE VIEW t_pbm_view AS SELECT k, v FROM t_pbm_base;
CREATE TABLE m_pbm_over_view ENGINE = Merge(currentDatabase(), '^t_pbm_(1|2|view)$');
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain = 'ReadFromMerge') > 0 AS merge_read_not_expanded
FROM (SELECT trimLeft(explain) AS explain FROM (EXPLAIN pretty = 0, description = 0 SELECT count(), sum(k), sum(v) FROM m_pbm_over_view));
SELECT count(), sum(k), sum(v) FROM m_pbm_over_view;
SELECT count(), sum(k), sum(v) FROM m_pbm_over_view SETTINGS enable_parallel_replicas = 0;

-- A read that would not be distributed anyway must not be expanded: the reads of these tables cannot be
-- shipped with `parallel_replicas_for_non_replicated_merge_tree = 0`, because a table which is not
-- replicated can hold different data on every replica. The `Merge` read is then left exactly as it is
-- instead of being turned into a union which nothing distributes.
SELECT '-- Merge over non-replicated tables which may not be read remotely';
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain = 'ReadFromMerge') > 0 AS merge_read_not_expanded
FROM (SELECT trimLeft(explain) AS explain FROM (EXPLAIN pretty = 0, description = 0 SELECT count(), sum(k), sum(v) FROM m_pbm))
SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;
SELECT count(), sum(k), sum(v) FROM m_pbm SETTINGS parallel_replicas_for_non_replicated_merge_tree = 0;

-- The reads of a union which reads one table twice cannot be coordinated: the coordinator drives every read
-- of a shipped fragment and cannot tell the two announcements of that table apart. Here the second branch
-- reads a table the `Merge` matches, so the expansion is what would create the duplicate - and the `Merge`
-- read is left as it is instead.
SELECT '-- a union which reads a child of the Merge again';
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain = 'ReadFromMerge') > 0 AS merge_read_not_expanded
FROM (SELECT trimLeft(explain) AS explain
      FROM (EXPLAIN pretty = 0, description = 0 SELECT count() FROM (SELECT k FROM m_pbm UNION ALL SELECT k FROM t_pbm_1)));
SELECT count(), sum(k) FROM (SELECT k FROM m_pbm UNION ALL SELECT k FROM t_pbm_1);
SELECT count(), sum(k) FROM (SELECT k FROM m_pbm UNION ALL SELECT k FROM t_pbm_1) SETTINGS enable_parallel_replicas = 0;

-- A `Merge` matching no table at all has nothing to distribute either.
SELECT '-- Merge over no tables';
CREATE TABLE m_pbm_over_nothing (k UInt64, v UInt64) ENGINE = Merge(currentDatabase(), '^t_pbm_no_such_tables');
SELECT
    countIf(explain LIKE '%ReadFromParallelReplicas%') > 0 AS has_remote_read,
    countIf(explain LIKE '%MergingAggregated%') > 0 AS has_merging_aggregated
FROM (EXPLAIN pretty = 0, description = 0 SELECT count(), sum(k), sum(v) FROM m_pbm_over_nothing);
SELECT count(), sum(k), sum(v) FROM m_pbm_over_nothing;

DROP TABLE m_pbm_over_nothing;
DROP TABLE m_pbm_over_view;
DROP VIEW t_pbm_view;
DROP TABLE t_pbm_base;
DROP TABLE m_pbm_over_log;
DROP TABLE t_pbm_log;
DROP TABLE m_pbm;
DROP TABLE t_pbm_2;
DROP TABLE t_pbm_1;
