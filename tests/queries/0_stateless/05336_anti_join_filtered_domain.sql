-- `NOT EXISTS` over a filtered table with several rows per key. A filter leaves a key value only
-- when one of its rows survives, two filtered sets of one key domain overlap in proportion to their
-- shares of it, and the anti join drops the preserved keys the other side has. Without the first
-- two the join below the anti join would see every preserved key matched and the anti join would
-- estimate one row.
--
-- `t_ae_fact` has four rows per key with a pseudo-random `v`; `v < 334` keeps a third of them, so
-- a key keeps at least one row with probability 1 - (2/3)^4 = 0.8 and the anti join keeps about a
-- fifth of the preserved rows.
SET allow_experimental_statistics = 1;
-- `NOT EXISTS` is an anti join only through this conversion; the runner randomizes it.
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
-- The columns a step does not need are dropped below it, which narrows the rows the costs count;
-- the runner randomizes the removal.
SET query_plan_remove_unused_columns = 1;
SET use_statistics = 1;
-- The test runner may turn this off; the inserted parts need their statistics.
SET materialize_statistics_on_insert = 1;
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
-- The plan lines below show the filter moved to PREWHERE; the runner randomizes the move.
SET optimize_move_to_prewhere = 1;
SET enable_parallel_replicas = 0;
SET allow_correlated_subqueries = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET collect_hash_table_stats_during_joins = 0;
SET max_rows_to_group_by = 0;
SET max_rows_in_join = 0;
SET max_bytes_in_join = 0;
SET param__internal_cascades_cluster_node_count = 3;

DROP TABLE IF EXISTS t_ae_fact;
DROP TABLE IF EXISTS t_ae_keys;

-- Sparse serialization changes the bytes a read costs; the runner randomizes it.
CREATE TABLE t_ae_fact (k UInt64 STATISTICS(uniq, basic), v UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_ae_keys (k UInt64 STATISTICS(uniq, basic))
    ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_ae_fact SELECT number % 10000, cityHash64(number) % 1000 FROM numbers(40000);
INSERT INTO t_ae_keys SELECT number FROM numbers(10000);

SELECT '-- rows of the anti join';
SELECT count() FROM t_ae_keys AS a WHERE NOT EXISTS (SELECT 1 FROM t_ae_fact AS f WHERE f.k = a.k AND f.v < 334);

SELECT '-- estimate';
EXPLAIN estimates = 1
SELECT count() FROM t_ae_keys AS a WHERE NOT EXISTS (SELECT 1 FROM t_ae_fact AS f WHERE f.k = a.k AND f.v < 334)
SETTINGS correlated_subqueries_use_in_memory_buffer = 1;

SELECT '-- Cascades';
EXPLAIN estimates = 1
SELECT count() FROM t_ae_keys AS a WHERE NOT EXISTS (SELECT 1 FROM t_ae_fact AS f WHERE f.k = a.k AND f.v < 334)
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, correlated_subqueries_use_in_memory_buffer = 0;

DROP TABLE t_ae_fact;
DROP TABLE t_ae_keys;
