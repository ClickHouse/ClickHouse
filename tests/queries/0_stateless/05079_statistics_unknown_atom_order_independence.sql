-- An atom the estimator cannot analyse (here a comparison between two columns) carries no range, so a
-- conjunctive merge cannot represent it. It is absorbed instead: its selectivity is kept aside and
-- applied at the end, which leaves the ranges around it free to merge with each other. Finalizing the
-- clause on the spot would make `b > 1000 AND <unknown> AND b < 2000` estimate the two bounds as
-- independent factors, while `b > 1000 AND b < 2000 AND <unknown>` intersects them - the same predicate
-- written in a different order would get a different estimate, and possibly a different join order.
--
-- The assertions compare the estimates against each other rather than against fixed numbers, so they
-- hold whatever the default factors are, and every estimate is required to be non-null so that a
-- failed extraction cannot satisfy the comparisons vacuously.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_atom_order;
DROP TABLE IF EXISTS t_atom_order_dim;
DROP TABLE IF EXISTS t_atom_order_estimates;

CREATE TABLE t_atom_order (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_atom_order_dim (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_atom_order_estimates (name String, estimated_rows Nullable(UInt64)) ENGINE = Memory;

-- Statistics are materialized by a merge, so two parts and an OPTIMIZE are required.
INSERT INTO t_atom_order SELECT number,         number % 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_atom_order SELECT number + 50000, number % 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_atom_order_dim SELECT number FROM numbers(150);
OPTIMIZE TABLE t_atom_order FINAL;

SELECT 'statistics materialized', max(level) >= 1
FROM system.parts WHERE database = currentDatabase() AND table = 't_atom_order' AND active;

-- Each estimate is the row count the join reorder derived for `t_atom_order` under the given
-- predicate. The join-order optimizer must be on and must use the real statistics:
-- `..._randomize` substitutes random cardinalities and `use_hash_table_stats_for_join_reordering`
-- can override them with row counts measured by earlier executions.

INSERT INTO t_atom_order_estimates
SELECT 'r1_u_r2', toUInt64OrNull(extract(explain, 't_atom_order\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_atom_order INNER JOIN t_atom_order_dim
      ON t_atom_order_dim.id = t_atom_order.a
      WHERE t_atom_order.b > 1000 AND t_atom_order.b > t_atom_order.c AND t_atom_order.b < 2000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_atom_order_estimates
SELECT 'r1_r2_u', toUInt64OrNull(extract(explain, 't_atom_order\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_atom_order INNER JOIN t_atom_order_dim
      ON t_atom_order_dim.id = t_atom_order.a
      WHERE t_atom_order.b > 1000 AND t_atom_order.b < 2000 AND t_atom_order.b > t_atom_order.c
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_atom_order_estimates
SELECT 'u_r1_r2', toUInt64OrNull(extract(explain, 't_atom_order\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_atom_order INNER JOIN t_atom_order_dim
      ON t_atom_order_dim.id = t_atom_order.a
      WHERE t_atom_order.b > t_atom_order.c AND t_atom_order.b > 1000 AND t_atom_order.b < 2000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_atom_order_estimates
SELECT 'u_r2_r1', toUInt64OrNull(extract(explain, 't_atom_order\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_atom_order INNER JOIN t_atom_order_dim
      ON t_atom_order_dim.id = t_atom_order.a
      WHERE t_atom_order.b > t_atom_order.c AND t_atom_order.b < 2000 AND t_atom_order.b > 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_atom_order_estimates
SELECT 'r2_r1_u', toUInt64OrNull(extract(explain, 't_atom_order\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_atom_order INNER JOIN t_atom_order_dim
      ON t_atom_order_dim.id = t_atom_order.a
      WHERE t_atom_order.b < 2000 AND t_atom_order.b > 1000 AND t_atom_order.b > t_atom_order.c
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_atom_order_estimates
SELECT 'r2_u_r1', toUInt64OrNull(extract(explain, 't_atom_order\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_atom_order INNER JOIN t_atom_order_dim
      ON t_atom_order_dim.id = t_atom_order.a
      WHERE t_atom_order.b < 2000 AND t_atom_order.b > t_atom_order.c AND t_atom_order.b > 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

SELECT 'every estimate was extracted', count() = 6 AND countIf(estimated_rows > 0) = 6
FROM t_atom_order_estimates;

-- The whole point: the estimate must not depend on the order the conjuncts were written in.
SELECT 'order does not change the estimate', uniqExact(estimated_rows) = 1
FROM t_atom_order_estimates;

DROP TABLE t_atom_order_estimates;
DROP TABLE t_atom_order_dim;
DROP TABLE t_atom_order;
