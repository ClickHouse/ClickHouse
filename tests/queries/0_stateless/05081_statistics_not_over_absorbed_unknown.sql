-- Negating a clause that absorbed an unknown atom cannot be done by flipping its ranges. A clause
-- that absorbed one is a conjunction times a constant factor, and `NOT (a AND u)` is `NOT a OR NOT u`,
-- not the flipped ranges times the same factor. Such a clause is finalized before the negation, so
-- `NOT` applies to a plain selectivity and the result is the complement of the un-negated clause.
--
-- The assertions compare estimates against each other rather than against fixed numbers, so they hold
-- whatever the default factors are, and every estimate is required to be non-null so that a failed
-- extraction cannot satisfy the comparisons vacuously.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_not_absorbed;
DROP TABLE IF EXISTS t_not_absorbed_dim;
DROP TABLE IF EXISTS t_not_absorbed_estimates;

CREATE TABLE t_not_absorbed (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_not_absorbed_dim (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_not_absorbed_estimates (name String, estimated_rows Nullable(UInt64)) ENGINE = Memory;

-- Statistics are materialized by a merge, so two parts and an OPTIMIZE are required.
INSERT INTO t_not_absorbed SELECT number,         number % 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_not_absorbed SELECT number + 50000, number % 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_not_absorbed_dim SELECT number FROM numbers(150);
OPTIMIZE TABLE t_not_absorbed FINAL;

SELECT 'statistics materialized', max(level) >= 1
FROM system.parts WHERE database = currentDatabase() AND table = 't_not_absorbed' AND active;

-- `b < 1000` is a range atom estimated from the column statistics; `b > c` compares two columns and
-- cannot be analysed, so it is an unknown atom that the conjunction absorbs.
INSERT INTO t_not_absorbed_estimates
SELECT 'clause', toUInt64OrNull(extract(explain, 't_not_absorbed\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_absorbed INNER JOIN t_not_absorbed_dim
      ON t_not_absorbed_dim.id = t_not_absorbed.a
      WHERE t_not_absorbed.b < 1000 AND t_not_absorbed.b > t_not_absorbed.c
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_not_absorbed_estimates
SELECT 'negated_clause', toUInt64OrNull(extract(explain, 't_not_absorbed\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_absorbed INNER JOIN t_not_absorbed_dim
      ON t_not_absorbed_dim.id = t_not_absorbed.a
      WHERE NOT (t_not_absorbed.b < 1000 AND t_not_absorbed.b > t_not_absorbed.c)
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

-- Control: the same shape with no unknown atom to absorb, which is negated by flipping the ranges
-- instead. It must satisfy the same relation.
INSERT INTO t_not_absorbed_estimates
SELECT 'range', toUInt64OrNull(extract(explain, 't_not_absorbed\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_absorbed INNER JOIN t_not_absorbed_dim
      ON t_not_absorbed_dim.id = t_not_absorbed.a
      WHERE t_not_absorbed.b < 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_not_absorbed_estimates
SELECT 'negated_range', toUInt64OrNull(extract(explain, 't_not_absorbed\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_absorbed INNER JOIN t_not_absorbed_dim
      ON t_not_absorbed_dim.id = t_not_absorbed.a
      WHERE NOT (t_not_absorbed.b < 1000)
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

SELECT 'every estimate was extracted', count() = 4 AND countIf(estimated_rows > 0) = 4
FROM t_not_absorbed_estimates;

-- `P(NOT x) = 1 - P(x)`, so the two estimates must add up to the table. The tolerance absorbs the
-- rounding of each estimate to whole rows; negating the flipped ranges instead of the finalized
-- selectivity leaves the absorbed factor applied to the complement, which is out by a factor of
-- `default_unknown_cond_factor` - far outside it.
SELECT 'negation of an absorbing clause is its complement',
       abs(toInt64((SELECT estimated_rows FROM t_not_absorbed_estimates WHERE name = 'clause')
                 + (SELECT estimated_rows FROM t_not_absorbed_estimates WHERE name = 'negated_clause'))
           - 100000) <= 2;

SELECT 'negation of a plain range is its complement',
       abs(toInt64((SELECT estimated_rows FROM t_not_absorbed_estimates WHERE name = 'range')
                 + (SELECT estimated_rows FROM t_not_absorbed_estimates WHERE name = 'negated_range'))
           - 100000) <= 2;

DROP TABLE t_not_absorbed_estimates;
DROP TABLE t_not_absorbed_dim;
DROP TABLE t_not_absorbed;
