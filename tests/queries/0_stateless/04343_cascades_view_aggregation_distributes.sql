-- Verifies that a distributed Cascades plan keeps a hash distribution across a VIEW boundary.
-- A view that aggregates is joined on its grouping key, so the join is a distributed Shuffle
-- join and the view's scan runs in parallel.  The VIEW read inserts a `materialize` node (the
-- "Materialize constants after VIEW subquery" step) which must stay transparent to
-- distribution-key tracking; otherwise the whole view subtree is funnelled onto a single node
-- (a Local HashJoin over a non-parallel read).

DROP TABLE IF EXISTS facts;
DROP TABLE IF EXISTS dims;
DROP VIEW IF EXISTS agg_view;

CREATE TABLE facts (key Int32, val Int64) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';
CREATE TABLE dims (key Int32, name String) ENGINE = MergeTree ORDER BY key
  SETTINGS auto_statistics_types = '';

-- Sentinel rows keep the plan from short-circuiting on empty tables.
INSERT INTO facts VALUES (1, 10);
INSERT INTO dims VALUES (1, 'a');

CREATE VIEW agg_view AS SELECT key AS k, sum(val) AS total FROM facts GROUP BY key;

SET allow_experimental_analyzer = 1;
SET make_distributed_plan = 1;
SET enable_cascades_optimizer = 1;
SET distributed_plan_execute_locally = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET max_rows_to_group_by = 0;

-- Simulate a 20 node cluster and weight the cost model towards parallelism.
SET param__internal_cascades_cluster_node_count = 20;
SET param__internal_cascades_cost_config = '{"work_weight":1,"exchange_fixed_overhead":100,"network_weight":1,"sequential_weight":1000}';
SET param__internal_join_table_stat_hints = '{
    "facts": { "cardinality": 21400000, "avg_row_bytes": 16, "distinct_keys": { "key": 1000000 } },
    "dims":  { "cardinality": 1000000,  "avg_row_bytes": 40, "distinct_keys": { "key": 1000000 } }
}';

EXPLAIN SELECT d.name, total FROM dims AS d, agg_view WHERE d.key = k ORDER BY d.name;

-- The distributed plan returns the same result as a single-node plan.
SELECT d.name, total FROM dims AS d, agg_view WHERE d.key = k ORDER BY d.name;

DROP VIEW agg_view;
DROP TABLE facts;
DROP TABLE dims;
