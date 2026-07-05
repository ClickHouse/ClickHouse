-- The Cascades optimizer must run only when `make_distributed_plan` is also
-- enabled (as documented in ARCHITECTURE.md). With `make_distributed_plan = 0`
-- the exchange steps it inserts are silently built as no-op pipeline steps, so
-- e.g. a partial aggregation created for `WITH TOTALS` reaches `TotalsHaving`
-- unmerged and produces duplicate groups.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
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

-- `force_aggregation_in_order` makes an in-order aggregation. It passes the Cascades
-- pre-check (the step sorts its own input, so exchanges below it are safe), but the plan
-- serializer cannot ship an in-order aggregation to workers, so the query is rejected
-- cleanly instead of returning wrong groups.
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

DROP TABLE t_gating;
