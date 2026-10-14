-- A `Merge` table expanded for plan-based parallel replicas is a union of the reads from its underlying
-- tables, so the number of tables reading at the same time is capped the way it is capped for a `UNION ALL`:
-- by `max_streams_for_union_step` and `max_streams_for_union_step_to_max_threads_ratio`, which insert
-- `Concat` processors above the reads. Without that cap a `Merge` over many tables would read all of them
-- simultaneously, which the reading from a `Merge` table has always avoided on the path where the children
-- are united at pipeline level.

DROP TABLE IF EXISTS m_pbmn;

CREATE TABLE t_pbmn_01 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_02 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_03 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_04 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_05 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_06 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_07 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_08 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_09 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_10 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_11 (k UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmn_12 (k UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_pbmn_01 SELECT number FROM numbers(100);
INSERT INTO t_pbmn_02 SELECT number + 100 FROM numbers(100);
INSERT INTO t_pbmn_03 SELECT number + 200 FROM numbers(100);
INSERT INTO t_pbmn_04 SELECT number + 300 FROM numbers(100);
INSERT INTO t_pbmn_05 SELECT number + 400 FROM numbers(100);
INSERT INTO t_pbmn_06 SELECT number + 500 FROM numbers(100);
INSERT INTO t_pbmn_07 SELECT number + 600 FROM numbers(100);
INSERT INTO t_pbmn_08 SELECT number + 700 FROM numbers(100);
INSERT INTO t_pbmn_09 SELECT number + 800 FROM numbers(100);
INSERT INTO t_pbmn_10 SELECT number + 900 FROM numbers(100);
INSERT INTO t_pbmn_11 SELECT number + 1000 FROM numbers(100);
INSERT INTO t_pbmn_12 SELECT number + 1100 FROM numbers(100);

CREATE TABLE m_pbmn ENGINE = Merge(currentDatabase(), '^t_pbmn_');

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_allow_merge_tables = 1;
SET parallel_replicas_local_plan = 1;
-- Pin the manual mode: otherwise CI's randomized automatic_parallel_replicas_mode can cost-decide against
-- parallel replicas for these small tables, so the plan-based split does not engage.
SET automatic_parallel_replicas_mode = 0;
SET max_threads = 1;

-- The number of reads is counted instead of matching the plan: which reads end up sharing a `Concat` is
-- decided by a shuffle, so the grouping the pipeline is printed with is not stable.
SELECT '-- narrowing enabled';
SELECT countIf(explain LIKE '%Concat%') > 0 AS has_concat
FROM (EXPLAIN PIPELINE SELECT count() FROM m_pbmn SETTINGS max_streams_for_union_step_to_max_threads_ratio = 1);

SELECT '-- narrowing disabled by the settings';
SELECT countIf(explain LIKE '%Concat%') > 0 AS has_concat
FROM (EXPLAIN PIPELINE SELECT count() FROM m_pbmn
      SETTINGS max_streams_for_union_step = 0, max_streams_for_union_step_to_max_threads_ratio = 0);

-- Narrowing concatenates the reads in an arbitrary order, so it is only allowed while nothing above the
-- union relies on each of its streams being sorted on its own. An `ORDER BY` with a `LIMIT` is exactly such
-- a consumer: the sort is shipped with the fragment (a per-replica sort and a local top-N, merged on the
-- initiator), and reading in order reaches
-- the underlying tables through the expanded `Merge`, so every branch of the union delivers a sorted stream
-- that a merge above it consumes. Narrowing is therefore switched off for that union, and the plan and the
-- pipeline below say so positively instead of just reporting the absence of a `Concat`.
SELECT '-- ORDER BY with LIMIT: the sort is shipped and the children are read in order';
SELECT arrayStringConcat(arrayCompact(groupArray(step)), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT k FROM m_pbmn ORDER BY k LIMIT 5
          SETTINGS optimize_read_in_order = 1, max_streams_for_union_step = 1,
                   max_streams_for_union_step_to_max_threads_ratio = 0)
    WHERE step IN ('Limit', 'Sorting', 'Union', 'ReadFromMerge', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);
SELECT
    countIf(explain LIKE '%ReadPoolParallelReplicasInOrder%') > 0 AS children_read_in_order,
    countIf(explain LIKE '%Concat%') > 0 AS has_concat
FROM (EXPLAIN PIPELINE SELECT k FROM m_pbmn ORDER BY k LIMIT 5
      SETTINGS optimize_read_in_order = 1, max_streams_for_union_step = 1,
               max_streams_for_union_step_to_max_threads_ratio = 0);
SELECT k FROM m_pbmn ORDER BY k LIMIT 5
SETTINGS optimize_read_in_order = 1, max_streams_for_union_step = 1,
         max_streams_for_union_step_to_max_threads_ratio = 0;

DROP TABLE m_pbmn;
DROP TABLE t_pbmn_12;
DROP TABLE t_pbmn_11;
DROP TABLE t_pbmn_10;
DROP TABLE t_pbmn_09;
DROP TABLE t_pbmn_08;
DROP TABLE t_pbmn_07;
DROP TABLE t_pbmn_06;
DROP TABLE t_pbmn_05;
DROP TABLE t_pbmn_04;
DROP TABLE t_pbmn_03;
DROP TABLE t_pbmn_02;
DROP TABLE t_pbmn_01;
