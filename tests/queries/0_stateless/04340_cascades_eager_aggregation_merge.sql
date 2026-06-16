-- Regression test: eager aggregation pushes a partial `Aggregating` below the join and places
-- `MergingAggregated` above it. The merge reads key and aggregate-state columns positionally
-- ([keys..., states...]) and requires `AggregatedChunkInfo`, but the join output carries the
-- other side's columns (wrong layout) and no chunk info. Without the layout projection and the
-- single-level tagging the merge aborts with "Bad cast ... to ColumnAggregateFunction" or
-- "Chunk info was not set for chunk in MergingAggregatedTransform".

SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET param__internal_cascades_cluster_node_count = 4;

DROP TABLE IF EXISTS t_orders;
DROP TABLE IF EXISTS t_lineitem;

CREATE TABLE t_orders (o_orderkey UInt64, o_custkey UInt64) ENGINE = MergeTree() ORDER BY o_orderkey;
CREATE TABLE t_lineitem (l_orderkey UInt64, l_quantity UInt64) ENGINE = MergeTree() ORDER BY l_orderkey;

INSERT INTO t_orders SELECT number, number % 10 FROM numbers(100);
INSERT INTO t_lineitem SELECT number % 100, number FROM numbers(10000);

-- GROUP BY = join key: eager aggregation pushes the partial aggregation of lineitem below the join.
SELECT '-- sum grouped by join key';
SELECT l_orderkey, sum(l_quantity) FROM t_orders JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY l_orderkey ORDER BY l_orderkey LIMIT 5;

-- Several aggregates exercise the [keys..., states...] layout with more than one state column.
SELECT '-- multiple aggregates grouped by join key';
SELECT l_orderkey, sum(l_quantity), count(), min(l_quantity), max(l_quantity)
FROM t_orders JOIN t_lineitem ON o_orderkey = l_orderkey
GROUP BY l_orderkey ORDER BY l_orderkey LIMIT 5;

DROP TABLE t_orders;
DROP TABLE t_lineitem;
