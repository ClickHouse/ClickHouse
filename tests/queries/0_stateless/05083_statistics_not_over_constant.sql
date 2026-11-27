-- A constant leaf is read back as a `function` tag: `AND`/`OR` drop or propagate a constant operand by
-- looking at the tag, and `NOT` flips it. Finalizing such a leaf only sets its selectivity, so a
-- negated constant that had been finalized would carry a number saying one thing and a tag saying the
-- opposite, and the folding acts on the tag. `indexHint` is the readily available constant here: it
-- returns 1 for every row, so the estimator treats it as always true and `NOT` of it as always false.
--
-- The assertions use the estimate of the range alone as the reference rather than a fixed number, so
-- they hold whatever the default factors are.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_not_const;
DROP TABLE IF EXISTS t_not_const_dim;
DROP TABLE IF EXISTS t_not_const_estimates;

CREATE TABLE t_not_const (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_not_const_dim (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_not_const_estimates (name String, estimated_rows Nullable(UInt64)) ENGINE = Memory;

-- Statistics are materialized by a merge, so two parts and an OPTIMIZE are required.
INSERT INTO t_not_const SELECT number,         number % 50000 FROM numbers(50000);
INSERT INTO t_not_const SELECT number + 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_not_const_dim SELECT number FROM numbers(150);
OPTIMIZE TABLE t_not_const FINAL;

SELECT 'statistics materialized', max(level) >= 1
FROM system.parts WHERE database = currentDatabase() AND table = 't_not_const' AND active;

INSERT INTO t_not_const_estimates
SELECT 'range', toUInt64OrNull(extract(explain, 't_not_const\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_const INNER JOIN t_not_const_dim
      ON t_not_const_dim.id = t_not_const.a
      WHERE t_not_const.b < 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

-- `NOT <always true> AND range` is never true.
INSERT INTO t_not_const_estimates
SELECT 'and_with_negated_constant', toUInt64OrNull(extract(explain, 't_not_const\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_const INNER JOIN t_not_const_dim
      ON t_not_const_dim.id = t_not_const.a
      WHERE NOT (indexHint(t_not_const.b < 1000)) AND t_not_const.b < 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

-- `NOT <always true> OR range` is exactly the range.
INSERT INTO t_not_const_estimates
SELECT 'or_with_negated_constant', toUInt64OrNull(extract(explain, 't_not_const\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_const INNER JOIN t_not_const_dim
      ON t_not_const_dim.id = t_not_const.a
      WHERE NOT (indexHint(t_not_const.b < 1000)) OR t_not_const.b < 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

-- One of the three is legitimately zero, so require non-null rather than positive.
SELECT 'every estimate was extracted', count() = 3 AND countIf(estimated_rows IS NOT NULL) = 3
FROM t_not_const_estimates;

-- Reading the negated constant as still-true leaves the conjunction estimated as the range alone.
SELECT 'a negated constant makes the conjunction empty',
       (SELECT estimated_rows FROM t_not_const_estimates WHERE name = 'and_with_negated_constant') = 0;

-- And makes the disjunction collapse to the negated constant instead of the range.
SELECT 'a negated constant leaves the disjunction at the range',
       (SELECT estimated_rows FROM t_not_const_estimates WHERE name = 'or_with_negated_constant')
     = (SELECT estimated_rows FROM t_not_const_estimates WHERE name = 'range');

DROP TABLE t_not_const_estimates;
DROP TABLE t_not_const_dim;
DROP TABLE t_not_const;
