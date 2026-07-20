-- Parallel replicas for Merge tables and the merge() table function inside JOINs and subqueries:
-- the query-level selector must designate the merge(...) leaf so that the whole join stage is
-- offloaded to replicas as a single WithMergeableState query.
-- https://github.com/ClickHouse/ClickHouse/issues/67770

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_prj_merge_1;
DROP TABLE IF EXISTS t_prj_merge_2;
DROP TABLE IF EXISTS t_prj_merge;
DROP TABLE IF EXISTS t_prj_dim;

CREATE TABLE t_prj_merge_1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_prj_merge_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
INSERT INTO t_prj_merge_1 SELECT number, number * 2 FROM numbers(10000);
INSERT INTO t_prj_merge_2 SELECT number + 10000, number FROM numbers(10000);

CREATE TABLE t_prj_merge ENGINE = Merge(currentDatabase(), '^t_prj_merge_[12]$');

CREATE TABLE t_prj_dim (k UInt64, name String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_prj_dim SELECT number * 100, 'dim_' || toString(number * 100) FROM numbers(200);

SET enable_analyzer = 1;
SET max_threads = 4;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_allow_merge_tables = 1;
SET automatic_parallel_replicas_mode = 0;

-- The whole join stage must be offloaded to replicas: the plan must contain a remote
-- parallel-replicas step whose query includes the JOIN itself.
SELECT '-- merge() INNER JOIN a MergeTree table: the join is offloaded';
SELECT count() FROM (EXPLAIN SELECT count(), sum(m.v) FROM merge(currentDatabase(), '^t_prj_merge_[12]$') AS m INNER JOIN t_prj_dim AS d ON m.k = d.k) WHERE trimLeft(explain) LIKE 'ReadFromRemoteParallelReplicas%INNER JOIN%';

SELECT '-- merge() joined in a subquery: the join is offloaded';
SELECT count() FROM (EXPLAIN SELECT sum(cnt) FROM (SELECT d.name AS name, count() AS cnt FROM merge(currentDatabase(), '^t_prj_merge_[12]$') AS m INNER JOIN t_prj_dim AS d ON m.k = d.k GROUP BY name)) WHERE trimLeft(explain) LIKE 'ReadFromRemoteParallelReplicas%INNER JOIN%GROUP BY%';

-- Slow the initiator's local reads so that remote replicas actually produce rows;
-- rows read both locally and remotely would then surface as wrong aggregates.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT '-- merge() INNER JOIN a MergeTree table';
SELECT count(), sum(m.v) FROM merge(currentDatabase(), '^t_prj_merge_[12]$') AS m INNER JOIN t_prj_dim AS d ON m.k = d.k;

SELECT '-- merge() LEFT JOIN a MergeTree table';
SELECT count(), sum(m.v), countIf(d.name != '') FROM merge(currentDatabase(), '^t_prj_merge_[12]$') AS m LEFT JOIN t_prj_dim AS d ON m.k = d.k;

SELECT '-- MergeTree table RIGHT JOIN merge()';
SELECT count(), sum(m.v) FROM t_prj_dim AS d RIGHT JOIN merge(currentDatabase(), '^t_prj_merge_[12]$') AS m ON d.k = m.k;

SELECT '-- Merge table INNER JOIN a MergeTree table';
SELECT count(), sum(m.v) FROM t_prj_merge AS m INNER JOIN t_prj_dim AS d ON m.k = d.k;

SELECT '-- merge() in a subquery';
SELECT sum(cnt), sum(s) FROM (SELECT k % 10 AS g, count() AS cnt, sum(v) AS s FROM merge(currentDatabase(), '^t_prj_merge_[12]$') GROUP BY g);

SELECT '-- merge() joined in a subquery';
SELECT sum(cnt) FROM (SELECT d.name AS name, count() AS cnt FROM merge(currentDatabase(), '^t_prj_merge_[12]$') AS m INNER JOIN t_prj_dim AS d ON m.k = d.k GROUP BY name);

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- FINAL disables parallel replicas; the query must fall back to single-replica execution
-- instead of failing.
SELECT '-- merge() FINAL joined';
SELECT count() FROM (EXPLAIN SELECT count(), sum(m.v) FROM merge(currentDatabase(), '^t_prj_merge_[12]$') AS m FINAL INNER JOIN t_prj_dim AS d ON m.k = d.k) WHERE trimLeft(explain) LIKE 'ReadFromRemoteParallelReplicas%';
SELECT count(), sum(m.v) FROM merge(currentDatabase(), '^t_prj_merge_[12]$') AS m FINAL INNER JOIN t_prj_dim AS d ON m.k = d.k;

DROP TABLE t_prj_dim;
DROP TABLE t_prj_merge;
DROP TABLE t_prj_merge_2;
DROP TABLE t_prj_merge_1;
