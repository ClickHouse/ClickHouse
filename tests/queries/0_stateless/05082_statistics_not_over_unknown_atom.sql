-- An atom the estimator cannot analyse carries no ranges, so negating it by flipping ranges - which is
-- how `NOT` negates an unfinalized element - leaves it exactly as it was, and it goes on to report the
-- selectivity of the un-negated predicate. Such an element is finalized before the negation instead,
-- so `NOT` applies to a plain selectivity. It stays absorbable afterwards, so the ranges around it
-- still merge with each other.
--
-- The assertions compare estimates against each other rather than against fixed numbers, so they hold
-- whatever the default factors are, and every estimate is required to be non-null so that a failed
-- extraction cannot satisfy the comparisons vacuously.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_not_unknown;
DROP TABLE IF EXISTS t_not_unknown_dim;
DROP TABLE IF EXISTS t_not_unknown_estimates;

CREATE TABLE t_not_unknown (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_not_unknown_dim (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_not_unknown_estimates (name String, estimated_rows Nullable(UInt64)) ENGINE = Memory;

-- Statistics are materialized by a merge, so two parts and an OPTIMIZE are required.
INSERT INTO t_not_unknown SELECT number,         number % 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_not_unknown SELECT number + 50000, number % 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_not_unknown_dim SELECT number FROM numbers(150);
OPTIMIZE TABLE t_not_unknown FINAL;

SELECT 'statistics materialized', max(level) >= 1
FROM system.parts WHERE database = currentDatabase() AND table = 't_not_unknown' AND active;

-- `b > c` compares two columns and cannot be analysed, so it is an unknown atom. `b < 1000` is an
-- ordinary range atom estimated from the column statistics.
INSERT INTO t_not_unknown_estimates
SELECT 'unknown', toUInt64OrNull(extract(explain, 't_not_unknown\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_unknown INNER JOIN t_not_unknown_dim
      ON t_not_unknown_dim.id = t_not_unknown.a
      WHERE t_not_unknown.b > t_not_unknown.c
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_not_unknown_estimates
SELECT 'negated_unknown', toUInt64OrNull(extract(explain, 't_not_unknown\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_unknown INNER JOIN t_not_unknown_dim
      ON t_not_unknown_dim.id = t_not_unknown.a
      WHERE NOT (t_not_unknown.b > t_not_unknown.c)
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_not_unknown_estimates
SELECT 'range', toUInt64OrNull(extract(explain, 't_not_unknown\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_unknown INNER JOIN t_not_unknown_dim
      ON t_not_unknown_dim.id = t_not_unknown.a
      WHERE t_not_unknown.b < 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_not_unknown_estimates
SELECT 'mixed', toUInt64OrNull(extract(explain, 't_not_unknown\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_unknown INNER JOIN t_not_unknown_dim
      ON t_not_unknown_dim.id = t_not_unknown.a
      WHERE NOT (t_not_unknown.b > t_not_unknown.c) AND t_not_unknown.b < 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

-- The negated atom must not stop the two ranges around it from merging into one.
INSERT INTO t_not_unknown_estimates
SELECT 'split_by_negated', toUInt64OrNull(extract(explain, 't_not_unknown\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_unknown INNER JOIN t_not_unknown_dim
      ON t_not_unknown_dim.id = t_not_unknown.a
      WHERE t_not_unknown.b > 1000 AND NOT (t_not_unknown.b > t_not_unknown.c) AND t_not_unknown.b < 2000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_not_unknown_estimates
SELECT 'ranges_adjacent', toUInt64OrNull(extract(explain, 't_not_unknown\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_not_unknown INNER JOIN t_not_unknown_dim
      ON t_not_unknown_dim.id = t_not_unknown.a
      WHERE t_not_unknown.b > 1000 AND t_not_unknown.b < 2000 AND NOT (t_not_unknown.b > t_not_unknown.c)
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

SELECT 'every estimate was extracted', count() = 6 AND countIf(estimated_rows > 0) = 6
FROM t_not_unknown_estimates;

-- `P(NOT x) = 1 - P(x)`. Leaving the atom unnegated reports `P(x)` for both, which sums to well under
-- the table unless the factor happens to be exactly one half.
SELECT 'negating an unknown atom gives its complement',
       abs(toInt64((SELECT estimated_rows FROM t_not_unknown_estimates WHERE name = 'unknown')
                 + (SELECT estimated_rows FROM t_not_unknown_estimates WHERE name = 'negated_unknown'))
           - 100000) <= 2;

-- The negated atom must compose with an ordinary range the same way the un-negated one does: the
-- estimator has no notion of correlation between them, so the conjunction is their product.
SELECT 'a negated unknown composes as its own estimate',
       abs(toInt64((SELECT estimated_rows FROM t_not_unknown_estimates WHERE name = 'mixed'))
           - toInt64((SELECT estimated_rows FROM t_not_unknown_estimates WHERE name = 'negated_unknown')
                   * (SELECT estimated_rows FROM t_not_unknown_estimates WHERE name = 'range') / 100000)) <= 2;

-- And it must still be absorbed, so that the ranges on either side of it merge into one.
SELECT 'a negated unknown does not split the ranges around it',
       (SELECT estimated_rows FROM t_not_unknown_estimates WHERE name = 'split_by_negated')
     = (SELECT estimated_rows FROM t_not_unknown_estimates WHERE name = 'ranges_adjacent');

DROP TABLE t_not_unknown_estimates;
DROP TABLE t_not_unknown_dim;
DROP TABLE t_not_unknown;
