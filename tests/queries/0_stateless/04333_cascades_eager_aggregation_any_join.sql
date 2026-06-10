-- `EagerAggregation` (Yan & Larson) is only valid for strictness ALL: with
-- `ANY INNER JOIN` the join keeps at most one match per row, but a partial
-- aggregate pushed below the join folds ALL matching rows into the state,
-- inflating sums and counts.

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_any_orders;
DROP TABLE IF EXISTS t_any_lineitem;

CREATE TABLE t_any_orders (o_orderkey UInt64) ENGINE = MergeTree() ORDER BY o_orderkey;
CREATE TABLE t_any_lineitem (l_orderkey UInt64, l_quantity UInt64) ENGINE = MergeTree() ORDER BY l_orderkey;

-- High fan-out: 100 lineitem rows per order key (the shape that makes eager aggregation win).
INSERT INTO t_any_orders SELECT number FROM numbers(100);
INSERT INTO t_any_lineitem SELECT number % 100, 1 FROM numbers(10000);

SELECT '-- 1. ANY INNER JOIN + sum: at most one match per left row';
SELECT sum(cnt), count() FROM (
    SELECT l_orderkey, sum(l_quantity) AS cnt
    FROM t_any_orders ANY JOIN t_any_lineitem ON o_orderkey = l_orderkey
    GROUP BY l_orderkey
);

SELECT '-- 2. Baseline without Cascades';
SELECT sum(cnt), count() FROM (
    SELECT l_orderkey, sum(l_quantity) AS cnt
    FROM t_any_orders ANY JOIN t_any_lineitem ON o_orderkey = l_orderkey
    GROUP BY l_orderkey
)
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE t_any_orders;
DROP TABLE t_any_lineitem;
