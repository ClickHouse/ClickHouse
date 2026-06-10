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

DROP TABLE t_gating;
