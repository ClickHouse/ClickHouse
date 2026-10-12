-- A semi join keeps the rows of its preserved side whose key values the other side has, an anti
-- join drops them: the fraction comes from the key NDVs, the other side's rows do not matter.
-- Without this both would estimate as outer joins, which keep the whole preserved side. The last
-- case runs the Cascades optimizer without the join order optimizer, so it estimates the anti join
-- itself. The hints stand in for column statistics.
SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET use_statistics = 0;
SET enable_join_runtime_filters = 0;
SET max_rows_to_group_by = 0;
SET param__internal_cascades_cluster_node_count = 3;
SET param__internal_join_table_stat_hints = '{"t_sa_fact": {"cardinality": 100000, "distinct_keys": {"k": 10000}}, "t_sa_dim": {"cardinality": 1000, "distinct_keys": {"k": 1000}}, "t_sa_many": {"cardinality": 100000, "distinct_keys": {"k": 1000}}}';

DROP TABLE IF EXISTS t_sa_fact;
DROP TABLE IF EXISTS t_sa_dim;
DROP TABLE IF EXISTS t_sa_many;

-- Sparse serialization changes the bytes a read costs; the runner randomizes it.
CREATE TABLE t_sa_fact (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_sa_dim (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
CREATE TABLE t_sa_many (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = '', ratio_of_defaults_for_sparse_serialization = 1;
INSERT INTO t_sa_fact SELECT number % 10000, number FROM numbers(1000);
INSERT INTO t_sa_dim SELECT number FROM numbers(1000);
INSERT INTO t_sa_many SELECT number % 1000, number FROM numbers(1000);

-- 1000 of the 10000 key values have a match: the semi join keeps 10000 rows, the anti join 90000.
SELECT '-- semi join against a table with unique keys';
EXPLAIN estimates = 1 SELECT count() FROM t_sa_fact AS f SEMI LEFT JOIN t_sa_dim AS d ON f.k = d.k;
SELECT '-- anti join against a table with unique keys';
EXPLAIN estimates = 1 SELECT count() FROM t_sa_fact AS f ANTI LEFT JOIN t_sa_dim AS d ON f.k = d.k;

-- The same 1000 key values, repeated over 100000 rows: the repeats add no matches.
SELECT '-- semi join against a table that repeats its keys';
EXPLAIN estimates = 1 SELECT count() FROM t_sa_fact AS f SEMI LEFT JOIN t_sa_many AS m ON f.k = m.k;
SELECT '-- anti join against a table that repeats its keys';
EXPLAIN estimates = 1 SELECT count() FROM t_sa_fact AS f ANTI LEFT JOIN t_sa_many AS m ON f.k = m.k;

SELECT '-- Cascades alone, anti join against a table that repeats its keys';
EXPLAIN estimates = 1 SELECT count() FROM t_sa_fact AS f ANTI LEFT JOIN t_sa_many AS m ON f.k = m.k
SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, query_plan_optimize_join_order_limit = 0;

DROP TABLE t_sa_fact;
DROP TABLE t_sa_dim;
DROP TABLE t_sa_many;
