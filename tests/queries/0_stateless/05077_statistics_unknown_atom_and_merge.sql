-- A predicate the estimator cannot analyse (here a comparison between two columns) becomes an unknown
-- atom, estimated with `default_unknown_cond_factor`. Merging a conjunction or a disjunction carries
-- only ranges across, and an unknown atom has none, so merging absorbed it without trace: it ended up
-- contributing a selectivity of 1 to an AND (the estimate degraded to the full table when both sides
-- were unknown) and 0 to an OR.
--
-- The assertions compare estimates against each other rather than against fixed numbers, so they hold
-- whatever the default factors are, and every estimate is required to be non-null so that a failed
-- extraction cannot satisfy the comparisons vacuously.
--
-- The estimates are collected one statement at a time. Collecting them as several `WITH` aliases makes
-- the old analyzer clone each alias into every nested subquery, which for an alias containing `EXPLAIN`
-- expands exponentially.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_unknown_atoms;
DROP TABLE IF EXISTS t_unknown_dim;
DROP TABLE IF EXISTS t_unknown_estimates;

CREATE TABLE t_unknown_atoms (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_unknown_dim (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_unknown_estimates (name String, estimated_rows Nullable(UInt64)) ENGINE = Memory;

-- Statistics are materialized by a merge, so two parts and an OPTIMIZE are required.
INSERT INTO t_unknown_atoms SELECT number, number, number FROM numbers(50000);
INSERT INTO t_unknown_atoms SELECT number + 50000, number, number FROM numbers(50000);
INSERT INTO t_unknown_dim SELECT number FROM numbers(100);
OPTIMIZE TABLE t_unknown_atoms FINAL;

SELECT 'statistics materialized', max(level) >= 1
FROM system.parts WHERE database = currentDatabase() AND table = 't_unknown_atoms' AND active;

-- Each estimate is the row count the join reorder derived for `t_unknown_atoms` under the given
-- predicate. `b > c` and `a > c` are column-to-column comparisons the estimator cannot analyse, so each
-- is an unknown atom; `b < 25000` is an ordinary range atom estimated from the column statistics.
-- The join-order optimizer must be on and must use the real statistics: `..._randomize` substitutes
-- random cardinalities and `use_hash_table_stats_for_join_reordering` can override them with row counts
-- measured by earlier executions. Settings randomization changes all three.
INSERT INTO t_unknown_estimates
SELECT 'one_unknown', toUInt64OrNull(extract(explain, 't_unknown_atoms\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
      SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
      WHERE t_unknown_atoms.b > t_unknown_atoms.c
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_unknown_estimates
SELECT 'two_unknown_and', toUInt64OrNull(extract(explain, 't_unknown_atoms\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
      SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
      WHERE t_unknown_atoms.b > t_unknown_atoms.c AND t_unknown_atoms.a > t_unknown_atoms.c
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_unknown_estimates
SELECT 'two_unknown_or', toUInt64OrNull(extract(explain, 't_unknown_atoms\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
      SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
      WHERE t_unknown_atoms.b > t_unknown_atoms.c OR t_unknown_atoms.a > t_unknown_atoms.c
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_unknown_estimates
SELECT 'range_only', toUInt64OrNull(extract(explain, 't_unknown_atoms\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
      SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
      WHERE t_unknown_atoms.b < 25000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_unknown_estimates
SELECT 'range_or_unknown', toUInt64OrNull(extract(explain, 't_unknown_atoms\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
      SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
      WHERE t_unknown_atoms.b < 25000 OR t_unknown_atoms.a > t_unknown_atoms.c
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

SELECT 'every estimate was extracted', count() = 5 AND countIf(estimated_rows > 0) = 5
FROM t_unknown_estimates;

SELECT 'one unknown atom is below the total',
       (SELECT estimated_rows FROM t_unknown_estimates WHERE name = 'one_unknown') < 100000;

-- AND: a second unknown conjunct must not make the relation look larger.
SELECT 'AND of two unknowns does not exceed one',
       (SELECT estimated_rows FROM t_unknown_estimates WHERE name = 'two_unknown_and')
    <= (SELECT estimated_rows FROM t_unknown_estimates WHERE name = 'one_unknown');

-- OR: a disjunction of two unknowns must not collapse to nothing, and is at least as wide as one.
SELECT 'OR of two unknowns is at least one',
       (SELECT estimated_rows FROM t_unknown_estimates WHERE name = 'two_unknown_or')
    >= (SELECT estimated_rows FROM t_unknown_estimates WHERE name = 'one_unknown');

-- OR with an analysable range: the unknown branch must widen the estimate, not be dropped.
SELECT 'OR of range and unknown exceeds the range alone',
       (SELECT estimated_rows FROM t_unknown_estimates WHERE name = 'range_or_unknown')
     > (SELECT estimated_rows FROM t_unknown_estimates WHERE name = 'range_only');

DROP TABLE t_unknown_estimates;
DROP TABLE t_unknown_atoms;
DROP TABLE t_unknown_dim;
