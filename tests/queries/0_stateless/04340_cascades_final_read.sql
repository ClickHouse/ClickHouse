-- Regression test: a distributed Cascades plan must not split a MergeTree read into per-node
-- mark-range buckets for FINAL on engines with specialized merging (ReplacingMergeTree, ...).
-- A shuffle join over `t_final FINAL` would otherwise read it with ParallelRead (bucketed), so
-- FINAL dedup runs per node and equal-key rows are never merged, double-counting v. The FINAL
-- read must stay serial (the same way tryMakeDistributedRead does for the non-Cascades plan).

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;
SET max_threads = 4;

DROP TABLE IF EXISTS t_final;
DROP TABLE IF EXISTS t_dim;

CREATE TABLE t_final (k UInt64, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY k;
-- Two parts with the same keys; FINAL keeps the latest version (v = 2).
INSERT INTO t_final SELECT number, 1 FROM numbers(100000);
INSERT INTO t_final SELECT number, 2 FROM numbers(100000);

CREATE TABLE t_dim (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_dim SELECT number FROM numbers(100000);

-- Shuffle join over FINAL. With correct (serial) FINAL dedup each key has v = 2, so
-- sum = 100000 * 2 = 200000. Bucketed FINAL would double-count to 300000.
SELECT '-- sum over shuffle join on FINAL';
SELECT sum(a.v) FROM t_final AS a FINAL JOIN t_dim AS b ON a.k = b.k;

DROP TABLE t_final;
DROP TABLE t_dim;
