-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- Regression test: a distributed Cascades plan must not split a FINAL MergeTree read into
-- arbitrary mark-range buckets on engines with specialized merging (ReplacingMergeTree, ...):
-- FINAL dedup would run per node and equal-key rows would never merge, double-counting v.
-- A FINAL read is bucketed only along primary-key-range layers, so a dedup group never spans
-- buckets; a read that cannot be split safely stays serial.

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

-- Cascades clones the read step; the clone must keep filters deferred until after FINAL
-- (`apply_prewhere_after_final`). A clone that loses them would filter before deduplication.
-- The outer query runs without Cascades (it reads from the `viewExplain` table function, which the
-- optimizer cannot clone); the inner EXPLAIN re-enables it explicitly.
SELECT '-- deferred prewhere survives the Cascades clone';
SELECT sum(explain LIKE '%Deferred prewhere filter column%') FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM t_final FINAL PREWHERE v = 1
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, apply_prewhere_after_final = 1
) SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
-- FINAL keeps v = 2 for every key, so the deferred filter `v = 1` leaves no rows.
SELECT count() FROM t_final FINAL PREWHERE v = 1
SETTINGS apply_prewhere_after_final = 1, distributed_plan_execute_locally = 1;
SELECT count() FROM t_final FINAL PREWHERE v = 1
SETTINGS apply_prewhere_after_final = 1, enable_cascades_optimizer = 0, make_distributed_plan = 0;

-- Row policies are deferred after FINAL by default (`apply_row_policy_after_final`);
-- the clone must keep that deferral too.
SELECT '-- deferred row policy survives the Cascades clone';
DROP ROW POLICY IF EXISTS policy_04340_deferred ON t_final;
CREATE ROW POLICY policy_04340_deferred ON t_final FOR SELECT USING v = 1 TO CURRENT_USER;
SELECT sum(explain LIKE '%Deferred row level filter column%') FROM (
    EXPLAIN PLAN actions = 1
    SELECT count() FROM t_final FINAL
    SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1
) SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
-- FINAL keeps v = 2, so the deferred policy `v = 1` leaves no rows.
SELECT count() FROM t_final FINAL SETTINGS distributed_plan_execute_locally = 1;
SELECT count() FROM t_final FINAL SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;
DROP ROW POLICY policy_04340_deferred ON t_final;

DROP TABLE t_final;
DROP TABLE t_dim;
