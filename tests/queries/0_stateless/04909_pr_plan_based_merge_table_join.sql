-- A `Merge` table inside a JOIN with plan-based parallel replicas. The `Merge` read is expanded into a
-- union of the reads from the underlying `MergeTree` tables, and the split marker is then lifted above
-- that union and above the join, so the whole join ships as one fragment: the coordinated side is read
-- across the replicas and the other side is read in full by each of them.
--
-- Every result is printed twice - with and without parallel replicas - so that the reference shows the
-- parity. The plan steps show *how* the query was distributed, which the presence of a
-- `ReadFromParallelReplicas` step alone cannot: `Union` before `Join` means the whole join shipped as one
-- fragment, `Join` before `ReadFromParallelReplicas` means the join stayed on the initiator with only the
-- coordinated read distributed, and no `ReadFromParallelReplicas` means the query is fully local.

DROP TABLE IF EXISTS t_pbmj_1;
DROP TABLE IF EXISTS t_pbmj_2;
DROP TABLE IF EXISTS m_pbmj;
DROP TABLE IF EXISTS t_pbmj_dim_1;
DROP TABLE IF EXISTS t_pbmj_dim_2;
DROP TABLE IF EXISTS m_pbmj_dim;
DROP TABLE IF EXISTS t_pbmj_dim;

CREATE TABLE t_pbmj_1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_pbmj_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
INSERT INTO t_pbmj_1 SELECT number, number * 2 FROM numbers(1000);
INSERT INTO t_pbmj_2 SELECT number + 1000, number FROM numbers(1000);
CREATE TABLE m_pbmj ENGINE = Merge(currentDatabase(), '^t_pbmj_[12]$');

CREATE TABLE t_pbmj_dim (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pbmj_dim SELECT number * 100, 'dim_' || toString(number * 100) FROM numbers(20);

-- A second `Merge` table, to join one `Merge` with another one.
CREATE TABLE t_pbmj_dim_1 (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pbmj_dim_2 (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pbmj_dim_1 SELECT number * 100, 'dim_' || toString(number * 100) FROM numbers(10);
INSERT INTO t_pbmj_dim_2 SELECT (number + 10) * 100, 'dim_' || toString((number + 10) * 100) FROM numbers(10);
CREATE TABLE m_pbmj_dim ENGINE = Merge(currentDatabase(), '^t_pbmj_dim_[12]$');

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_plan_based = 1;
SET parallel_replicas_allow_merge_tables = 1;
-- Pin the manual mode: otherwise CI's randomized automatic_parallel_replicas_mode can cost-decide against
-- parallel replicas for these small tables, so the plan-based split does not engage.
SET automatic_parallel_replicas_mode = 0;
-- Pin the plan shape: the local plan must be present to hold the join step, and a randomized join order
-- changes which side is coordinated.
SET parallel_replicas_local_plan = 1;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';

-- Slow down the initiator's local read so that the remote replicas actually produce rows: rows read both
-- locally and remotely would then surface as wrong results.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT '-- Merge INNER JOIN MergeTree, the Merge is the coordinated side';
SELECT count(), sum(m.v) FROM m_pbmj AS m INNER JOIN t_pbmj_dim AS d ON m.k = d.k;
SELECT count(), sum(m.v) FROM m_pbmj AS m INNER JOIN t_pbmj_dim AS d ON m.k = d.k SETTINGS enable_parallel_replicas = 0;
SELECT arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count(), sum(m.v) FROM m_pbmj AS m INNER JOIN t_pbmj_dim AS d ON m.k = d.k)
    WHERE step IN ('Aggregating', 'MergingAggregated', 'Union', 'Join', 'ReadFromMerge', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

SELECT '-- Merge LEFT JOIN MergeTree';
SELECT count(), sum(m.v), countIf(d.name != '') FROM m_pbmj AS m LEFT JOIN t_pbmj_dim AS d ON m.k = d.k;
SELECT count(), sum(m.v), countIf(d.name != '') FROM m_pbmj AS m LEFT JOIN t_pbmj_dim AS d ON m.k = d.k SETTINGS enable_parallel_replicas = 0;

SELECT '-- MergeTree RIGHT JOIN Merge, the Merge is the coordinated side';
SELECT count(), sum(m.v) FROM t_pbmj_dim AS d RIGHT JOIN m_pbmj AS m ON d.k = m.k;
SELECT count(), sum(m.v) FROM t_pbmj_dim AS d RIGHT JOIN m_pbmj AS m ON d.k = m.k SETTINGS enable_parallel_replicas = 0;
SELECT arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count(), sum(m.v) FROM t_pbmj_dim AS d RIGHT JOIN m_pbmj AS m ON d.k = m.k)
    WHERE step IN ('Aggregating', 'MergingAggregated', 'Union', 'Join', 'ReadFromMerge', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

-- The `Merge` is on the side which is not coordinated: it is read in full by every replica, so its
-- expansion must not make the fragment read it more than once.
SELECT '-- MergeTree INNER JOIN Merge, the Merge is the broadcast side';
SELECT count(), sum(m.v) FROM t_pbmj_dim AS d INNER JOIN m_pbmj AS m ON d.k = m.k;
SELECT count(), sum(m.v) FROM t_pbmj_dim AS d INNER JOIN m_pbmj AS m ON d.k = m.k SETTINGS enable_parallel_replicas = 0;
SELECT arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count(), sum(m.v) FROM t_pbmj_dim AS d INNER JOIN m_pbmj AS m ON d.k = m.k)
    WHERE step IN ('Aggregating', 'MergingAggregated', 'Union', 'Join', 'ReadFromMerge', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

SELECT '-- Merge INNER JOIN Merge';
SELECT count(), sum(m.v) FROM m_pbmj AS m INNER JOIN m_pbmj_dim AS d ON m.k = d.k;
SELECT count(), sum(m.v) FROM m_pbmj AS m INNER JOIN m_pbmj_dim AS d ON m.k = d.k SETTINGS enable_parallel_replicas = 0;
SELECT arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count(), sum(m.v) FROM m_pbmj AS m INNER JOIN m_pbmj_dim AS d ON m.k = d.k)
    WHERE step IN ('Aggregating', 'MergingAggregated', 'Union', 'Join', 'ReadFromMerge', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

-- A `FULL` join is not distributed at all: concatenating the per-replica results of one coordinated side
-- would duplicate the unmatched rows of the other one. The `Merge` read must then be left as it is, instead
-- of being expanded into a union which nothing distributes.
SELECT '-- Merge FULL JOIN MergeTree, nothing is distributed';
SELECT count(), sum(m.v) FROM m_pbmj AS m FULL JOIN t_pbmj_dim AS d ON m.k = d.k;
SELECT count(), sum(m.v) FROM m_pbmj AS m FULL JOIN t_pbmj_dim AS d ON m.k = d.k SETTINGS enable_parallel_replicas = 0;
SELECT arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count(), sum(m.v) FROM m_pbmj AS m FULL JOIN t_pbmj_dim AS d ON m.k = d.k)
    WHERE step IN ('Aggregating', 'MergingAggregated', 'Union', 'Join', 'ReadFromMerge', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

SELECT '-- merge() table function INNER JOIN MergeTree';
SELECT count(), sum(m.v) FROM merge(currentDatabase(), '^t_pbmj_[12]$') AS m INNER JOIN t_pbmj_dim AS d ON m.k = d.k;
SELECT count(), sum(m.v) FROM merge(currentDatabase(), '^t_pbmj_[12]$') AS m INNER JOIN t_pbmj_dim AS d ON m.k = d.k SETTINGS enable_parallel_replicas = 0;

SELECT '-- the join of a Merge in a subquery';
SELECT sum(cnt) FROM (SELECT d.name AS name, count() AS cnt FROM m_pbmj AS m INNER JOIN t_pbmj_dim AS d ON m.k = d.k GROUP BY name);
SELECT sum(cnt) FROM (SELECT d.name AS name, count() AS cnt FROM m_pbmj AS m INNER JOIN t_pbmj_dim AS d ON m.k = d.k GROUP BY name) SETTINGS enable_parallel_replicas = 0;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- `FINAL` is incompatible with parallel reading: the whole query stays on a single replica.
SELECT '-- Merge FINAL joined';
SELECT count(), sum(m.v) FROM m_pbmj AS m FINAL INNER JOIN t_pbmj_dim AS d ON m.k = d.k;
SELECT count(), sum(m.v) FROM m_pbmj AS m FINAL INNER JOIN t_pbmj_dim AS d ON m.k = d.k SETTINGS enable_parallel_replicas = 0;
SELECT arrayStringConcat(groupArray(step), ' ')
FROM (
    SELECT trimLeft(explain) AS step
    FROM (EXPLAIN actions = 0, pretty = 0, optimize = 1, description = 0, header = 0
          SELECT count(), sum(m.v) FROM m_pbmj AS m FINAL INNER JOIN t_pbmj_dim AS d ON m.k = d.k)
    WHERE step IN ('Aggregating', 'MergingAggregated', 'Union', 'Join', 'ReadFromMerge', 'ReadFromMergeTree', 'ReadFromParallelReplicas')
);

DROP TABLE m_pbmj_dim;
DROP TABLE t_pbmj_dim_2;
DROP TABLE t_pbmj_dim_1;
DROP TABLE t_pbmj_dim;
DROP TABLE m_pbmj;
DROP TABLE t_pbmj_2;
DROP TABLE t_pbmj_1;
