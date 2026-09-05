-- A predicate the estimator cannot analyse (here a comparison between two columns) becomes an unknown
-- atom, estimated with `default_unknown_cond_factor`. Merging a conjunction or a disjunction carries
-- only ranges across, and an unknown atom has none, so merging absorbed it without trace: it ended up
-- contributing a selectivity of 1 to an AND (the estimate degraded to the full table when both sides
-- were unknown) and 0 to an OR.
--
-- The assertions compare estimates against each other rather than against fixed numbers, so they hold
-- whatever the default factors are. Every estimate is also required to be non-zero, otherwise a failed
-- extraction would satisfy the comparisons vacuously.

DROP TABLE IF EXISTS t_unknown_atoms;
DROP TABLE IF EXISTS t_unknown_dim;

CREATE TABLE t_unknown_atoms (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_unknown_dim (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic, uniq_v2';

-- Statistics are materialized by a merge, so two parts and an OPTIMIZE are required.
INSERT INTO t_unknown_atoms SELECT number, number, number FROM numbers(50000);
INSERT INTO t_unknown_atoms SELECT number + 50000, number, number FROM numbers(50000);
INSERT INTO t_unknown_dim SELECT number FROM numbers(100);
OPTIMIZE TABLE t_unknown_atoms FINAL;

SELECT 'statistics materialized', max(level) >= 1
FROM system.parts WHERE database = currentDatabase() AND table = 't_unknown_atoms' AND active;

-- The join-order optimizer must be on and must use the real statistics for the estimates to be
-- printed and meaningful: `query_plan_optimize_join_order_randomize` substitutes random
-- cardinalities, and `use_hash_table_stats_for_join_reordering` can override them with row
-- counts measured by earlier executions. Settings randomization changes all three.
-- `estimate` is the row count the join reorder derived for `t_unknown_atoms` under the given predicate.
-- `b > c` and `a > c` are column-to-column comparisons: the estimator cannot analyse either, so each is
-- an unknown atom. `b < 25000` is an ordinary range atom, estimated from the column statistics.
CREATE VIEW v_estimates AS
WITH
    (SELECT toUInt64OrNull(extract(explain, 't_unknown_atoms\\[(\\d+)\\]'))
     FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
           SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
           WHERE t_unknown_atoms.b > t_unknown_atoms.c
           SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
                    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
                    use_hash_table_stats_for_join_reordering = 0)
     WHERE explain LIKE '%Join:%' LIMIT 1) AS one_unknown,
    (SELECT toUInt64OrNull(extract(explain, 't_unknown_atoms\\[(\\d+)\\]'))
     FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
           SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
           WHERE t_unknown_atoms.b > t_unknown_atoms.c AND t_unknown_atoms.a > t_unknown_atoms.c
           SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
                    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
                    use_hash_table_stats_for_join_reordering = 0)
     WHERE explain LIKE '%Join:%' LIMIT 1) AS two_unknown_and,
    (SELECT toUInt64OrNull(extract(explain, 't_unknown_atoms\\[(\\d+)\\]'))
     FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
           SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
           WHERE t_unknown_atoms.b > t_unknown_atoms.c OR t_unknown_atoms.a > t_unknown_atoms.c
           SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
                    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
                    use_hash_table_stats_for_join_reordering = 0)
     WHERE explain LIKE '%Join:%' LIMIT 1) AS two_unknown_or,
    (SELECT toUInt64OrNull(extract(explain, 't_unknown_atoms\\[(\\d+)\\]'))
     FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
           SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
           WHERE t_unknown_atoms.b < 25000
           SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
                    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
                    use_hash_table_stats_for_join_reordering = 0)
     WHERE explain LIKE '%Join:%' LIMIT 1) AS range_only,
    (SELECT toUInt64OrNull(extract(explain, 't_unknown_atoms\\[(\\d+)\\]'))
     FROM (EXPLAIN PLAN keep_logical_steps = 1, actions = 1
           SELECT count() FROM t_unknown_atoms INNER JOIN t_unknown_dim ON t_unknown_dim.id = t_unknown_atoms.a
           WHERE t_unknown_atoms.b < 25000 OR t_unknown_atoms.a > t_unknown_atoms.c
           SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
                    query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
                    use_hash_table_stats_for_join_reordering = 0)
     WHERE explain LIKE '%Join:%' LIMIT 1) AS range_or_unknown
SELECT one_unknown, two_unknown_and, two_unknown_or, range_only, range_or_unknown;

SELECT 'every estimate was extracted',
       one_unknown > 0 AND two_unknown_and > 0 AND two_unknown_or > 0 AND range_only > 0 AND range_or_unknown > 0
FROM v_estimates;

SELECT 'one unknown atom is below the total', one_unknown < 100000 FROM v_estimates;

-- AND: a second unknown conjunct must not make the relation look larger.
SELECT 'AND of two unknowns does not exceed one', two_unknown_and <= one_unknown FROM v_estimates;

-- OR: a disjunction of two unknowns must not collapse to nothing, and is at least as wide as one.
SELECT 'OR of two unknowns is at least one', two_unknown_or >= one_unknown FROM v_estimates;

-- OR with an analysable range: the unknown branch must widen the estimate, not be dropped.
SELECT 'OR of range and unknown exceeds the range alone', range_or_unknown > range_only FROM v_estimates;

DROP VIEW v_estimates;
DROP TABLE t_unknown_atoms;
DROP TABLE t_unknown_dim;
