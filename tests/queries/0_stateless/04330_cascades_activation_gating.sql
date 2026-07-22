-- The Cascades optimizer must run only when `make_distributed_plan` is also
-- enabled (as documented in ARCHITECTURE.md). With `make_distributed_plan = 0`
-- the exchange steps it inserts are silently built as no-op pipeline steps, so
-- e.g. a partial aggregation created for `WITH TOTALS` reaches `TotalsHaving`
-- unmerged and produces duplicate groups.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_gating;

CREATE TABLE t_gating (k UInt64, x UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_gating SELECT number % 5, number FROM numbers(1000);

SELECT '-- 1. WITH TOTALS with cascades on but make_distributed_plan off: must be a plain single-node query';
SELECT k, sum(x) FROM t_gating GROUP BY k WITH TOTALS ORDER BY k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 0;

SELECT '-- 2. Plain aggregation, same settings';
SELECT k, sum(x) FROM t_gating GROUP BY k ORDER BY k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 0;

SELECT '-- 3. With both settings on, WITH TOTALS is rejected (fail-close)';
SELECT k, sum(x) FROM t_gating GROUP BY k WITH TOTALS ORDER BY k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }

-- A LOCAL JOIN must use only co-located data; a distributed Cascades plan cannot
-- guarantee that, so it is rejected.
SELECT '-- 4. LOCAL JOIN is rejected (fail-close)';
DROP TABLE IF EXISTS t_gating_dim;
CREATE TABLE t_gating_dim (k UInt64) ENGINE = MergeTree() ORDER BY k;
INSERT INTO t_gating_dim SELECT number % 5 FROM numbers(10);
SELECT count() FROM t_gating AS a LOCAL INNER JOIN t_gating_dim AS b ON a.k = b.k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM t_gating AS a LOCAL INNER JOIN t_gating_dim AS b ON a.k = b.k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 0;
DROP TABLE t_gating_dim;

-- `force_aggregation_in_order` makes an in-order aggregation, which assumes its input arrives
-- ordered by the group keys - the exchanges Cascades inserts do not preserve that. The pre-check
-- rejects it cleanly instead of building a plan that would return wrong groups.
SELECT '-- 5. force_aggregation_in_order is rejected (in-order aggregation is not serializable)';
SELECT k, sum(x) FROM t_gating GROUP BY k ORDER BY k
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    force_aggregation_in_order = 1, distributed_plan_execute_locally = 1; -- { serverError SUPPORT_IS_DISABLED }

-- The trivial-count rewrite is disabled under Cascades (its `ReadFromPreparedSource` leaf
-- cannot be cloned); the count runs as a distributed read instead.
SELECT '-- 6. Trivial count works under Cascades';
SELECT count() FROM t_gating
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1;

-- Reads without clone support (e.g. the `viewExplain` table function) are rejected up front.
SELECT '-- 7. A read without clone support is rejected (fail-close)';
SELECT count() FROM (EXPLAIN PLAN SELECT 1)
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }

-- A distributed read is bucketed and cannot be served from a projection, so projections are
-- turned off under `make_distributed_plan`; a forced projection is ignored and the query still works.
SELECT '-- 8. A forced projection is ignored (still correct)';
DROP TABLE IF EXISTS t_gating_proj;
CREATE TABLE t_gating_proj (a UInt64, b UInt64, PROJECTION p_agg (SELECT b, sum(a) GROUP BY b))
ENGINE = MergeTree ORDER BY a;
INSERT INTO t_gating_proj SELECT number, number % 5 FROM numbers(1000);
SELECT b, sum(a) FROM t_gating_proj GROUP BY b ORDER BY b
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    distributed_plan_execute_locally = 1, optimize_use_projections = 1, force_optimize_projection = 1;
DROP TABLE t_gating_proj;

-- A read from a `Distributed` table with remote shards fans out by itself and cannot be part
-- of the distributed plan; it is rejected up front. (With localhost shards the shard subplans
-- are inlined and planned locally, so the same query works.)
SELECT '-- 9. A read from remote shards is rejected (fail-close), localhost shards work';
DROP TABLE IF EXISTS t_gating_dist;
CREATE TABLE t_gating_dist AS t_gating ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_gating);
SELECT count() FROM t_gating_dist
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    prefer_localhost_replica = 0; -- { serverError SUPPORT_IS_DISABLED }
SELECT count() FROM t_gating_dist
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    prefer_localhost_replica = 1, distributed_plan_execute_locally = 1;
DROP TABLE t_gating_dist;

-- `WITH FILL` is not supported yet; without `make_distributed_plan` the same query runs single-node.
SELECT '-- 10. WITH FILL is rejected (fail-close)';
SELECT k FROM t_gating WHERE k IN (0, 2, 4) GROUP BY k ORDER BY k WITH FILL
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1; -- { serverError SUPPORT_IS_DISABLED }
SELECT k FROM t_gating WHERE k IN (0, 2, 4) GROUP BY k ORDER BY k WITH FILL
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 0;

DROP TABLE t_gating;
