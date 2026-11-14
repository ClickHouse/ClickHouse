-- Regression: `WITH CLUSTER` keeps the aggregation non-final so that `ClusterMergingStep` can merge
-- the exact groups afterwards. The partial-aggregation Top-K pushdown (`enable_group_by_top_k_optimization`)
-- is applied to non-final aggregations with `ORDER BY ... LIMIT`, and its heap evicts the groups that
-- fall outside the limit -- before the cluster step has seen them. It must be disabled for `WITH CLUSTER`.
-- See https://github.com/ClickHouse/ClickHouse/pull/101878

SET enable_analyzer = 1; -- `WITH CLUSTER` is implemented for the analyzer only
SET allow_experimental_group_by_with_cluster = 1;
SET enable_group_by_top_k_optimization = 1;

-- 1 and 2 are within distance 1, so they form a single cluster represented by 1 with count 2.
-- A Top-K heap of size 1 ordered by `x` would evict the exact group `x = 2` first and yield `(1, 1)`.
SELECT 'order by key limit';
SELECT x, count() FROM VALUES('x UInt8', (1), (2)) GROUP BY x WITH CLUSTER 1 ORDER BY x LIMIT 1;

SELECT 'order by key desc limit';
SELECT x, count() FROM VALUES('x UInt8', (1), (2), (2)) GROUP BY x WITH CLUSTER 1 ORDER BY x DESC LIMIT 1;

SELECT 'order by key limit with offset';
SELECT x, count() FROM VALUES('x UInt8', (1), (2), (10), (11), (11)) GROUP BY x WITH CLUSTER 1 ORDER BY x LIMIT 1 OFFSET 1;

-- Many exact groups collapsing into one cluster: every row must survive into the single cluster.
SELECT 'many groups one cluster';
SELECT x, count() FROM (SELECT toUInt64(number + 1) AS x FROM numbers(100)) GROUP BY x WITH CLUSTER 1 ORDER BY x LIMIT 1;

-- 2D and string keys use the same planner path.
SELECT '2d';
SELECT (x, y), count() FROM VALUES('x Int32, y Int32', (0, 0), (1, 1), (100, 100)) GROUP BY (x, y) WITH CLUSTER 2 ORDER BY (x, y) LIMIT 1;

SELECT 'string';
SELECT s, count() FROM VALUES('s String', ('abc'), ('abd'), ('zzzz')) GROUP BY s WITH CLUSTER 1 ORDER BY s LIMIT 1;

-- The plan must not carry a Top-K annotation on the aggregation step.
SELECT 'no top-k in plan';
SELECT countIf(explain ILIKE '%top%k%')
FROM (EXPLAIN PLAN actions = 1 SELECT x, count() FROM VALUES('x UInt8', (1), (2)) GROUP BY x WITH CLUSTER 1 ORDER BY x LIMIT 1);
