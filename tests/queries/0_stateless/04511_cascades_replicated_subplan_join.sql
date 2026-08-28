-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- A join of two small dimension tables that feeds a broadcast join is recomputed on every
-- node (a `Replicated` join over two `ReplicatedRead`s, no exchange) instead of being joined
-- on one node and broadcast.  A non-deterministic join condition disables the recomputation
-- (per-node results could diverge), falling back to the broadcast of a single-node join.
--
-- The result queries also guard the pinned-read parameter naming: the fact-side `ParallelRead`
-- and the two dimension `ReplicatedRead`s share one worker fragment, and each pins its marks in
-- its own task parameter.  A shared parameter key would make the fragment builder throw on the
-- conflicting values, so a passing distributed run proves the keys stay distinct.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_cascades_optimizer = 1;
SET make_distributed_plan = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET enable_join_runtime_filters = 0;
-- The Fast test profile sets a non-zero max_rows_to_group_by, which keeps aggregations local.
SET max_rows_to_group_by = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
-- The test pins full EXPLAIN outputs, so the randomized settings that shape these plans
-- are pinned to their defaults.
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;
SET query_plan_merge_filters = 1;
SET query_plan_merge_filter_into_join_condition = 1;
SET query_plan_merge_expression_into_join = 1;
SET query_plan_remove_unused_columns = 1;
SET enable_join_transitive_predicates = 1;
SET param__internal_cascades_cluster_node_count = 4;
SET param__internal_cascades_cost_config = '{"sequential_weight":32,"network_weight":1,"exchange_fixed_overhead":1,"work_weight":1}';

DROP TABLE IF EXISTS rsj_fact;
DROP TABLE IF EXISTS rsj_dim1;
DROP TABLE IF EXISTS rsj_dim2;
CREATE TABLE rsj_fact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE rsj_dim1 (k UInt64, g UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE rsj_dim2 (g UInt64, name String) ENGINE = MergeTree ORDER BY g SETTINGS auto_statistics_types = '';

-- The hints claim a huge fact table and tiny dimensions, so the dimension join is the
-- broadcast-join build side; the physical tables stay small to keep the test fast.
SET param__internal_join_table_stat_hints = '{"rsj_fact": {"cardinality": 10000000, "avg_row_bytes": 16, "distinct_keys": {"k": 1000}}, "rsj_dim1": {"cardinality": 1000, "avg_row_bytes": 16, "distinct_keys": {"k": 1000, "g": 25}}, "rsj_dim2": {"cardinality": 25, "avg_row_bytes": 24, "distinct_keys": {"g": 25}}}';

INSERT INTO rsj_fact SELECT number % 1000, number FROM numbers(100000);
INSERT INTO rsj_dim1 SELECT number, number % 25 FROM numbers(1000);
INSERT INTO rsj_dim2 SELECT number, toString(number) FROM numbers(25);

SELECT '-- 1. The dimension join is recomputed per node: a Replicated join over ReplicatedReads, no exchange';
EXPLAIN PLAN
SELECT count(), sum(f.v)
FROM rsj_fact AS f
JOIN (SELECT d1.k AS k, d2.name AS name FROM rsj_dim1 AS d1 JOIN rsj_dim2 AS d2 ON d1.g = d2.g) AS d
ON f.k = d.k
WHERE d.name != '';

SELECT '-- 2. Results match the non-distributed baseline';
SELECT count(), sum(f.v)
FROM rsj_fact AS f
JOIN (SELECT d1.k AS k, d2.name AS name FROM rsj_dim1 AS d1 JOIN rsj_dim2 AS d2 ON d1.g = d2.g) AS d
ON f.k = d.k
WHERE d.name != '';

SELECT count(), sum(f.v)
FROM rsj_fact AS f
JOIN (SELECT d1.k AS k, d2.name AS name FROM rsj_dim1 AS d1 JOIN rsj_dim2 AS d2 ON d1.g = d2.g) AS d
ON f.k = d.k
WHERE d.name != ''
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

SELECT '-- 3. A non-deterministic join condition falls back to broadcasting a single-node join';
-- The condition must not be provably true and must use columns from both join sides;
-- otherwise the optimizer removes it from the join or moves it into a filter, and the join
-- itself becomes deterministic and correctly runs replicated.
EXPLAIN PLAN
SELECT count(), sum(f.v)
FROM rsj_fact AS f
JOIN (SELECT d1.k AS k, d2.name AS name FROM rsj_dim1 AS d1 JOIN rsj_dim2 AS d2 ON d1.g = d2.g AND d1.g + (rand() % 100) >= d2.g) AS d
ON f.k = d.k
WHERE d.name != '';

SELECT '-- 4. Outer join kinds are eligible too: results match the baseline';
SELECT count()
FROM rsj_fact AS f
JOIN (SELECT d1.k AS k, d2.name AS name FROM rsj_dim1 AS d1 RIGHT JOIN rsj_dim2 AS d2 ON d1.g = d2.g) AS d
ON f.k = d.k;

SELECT count()
FROM rsj_fact AS f
JOIN (SELECT d1.k AS k, d2.name AS name FROM rsj_dim1 AS d1 RIGHT JOIN rsj_dim2 AS d2 ON d1.g = d2.g) AS d
ON f.k = d.k
SETTINGS enable_cascades_optimizer = 0, make_distributed_plan = 0;

DROP TABLE rsj_fact;
DROP TABLE rsj_dim1;
DROP TABLE rsj_dim2;
