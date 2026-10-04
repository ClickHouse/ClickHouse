-- A conjunction of atoms that carry no ranges - two column-to-column comparisons here - holds their
-- combined selectivity aside as a factor rather than as ranges. Negating it finalizes the clause,
-- folding that factor into its selectivity. The clause is then a number, and a conjunction it takes
-- part in afterwards has to use that number alone: carrying the factor over as well would apply it
-- twice, once on its own and once inside the complement it is already part of.
--
-- The assertions compare estimates against each other rather than against fixed numbers, so they hold
-- whatever the default factors are, and every estimate is required to be non-null so that a failed
-- extraction cannot satisfy the comparisons vacuously.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_absorb_reuse;
DROP TABLE IF EXISTS t_absorb_reuse_dim;
DROP TABLE IF EXISTS t_absorb_reuse_estimates;

CREATE TABLE t_absorb_reuse (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_absorb_reuse_dim (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_absorb_reuse_estimates (name String, estimated_rows Nullable(UInt64)) ENGINE = Memory;

-- Statistics are materialized by a merge, so two parts and an OPTIMIZE are required.
INSERT INTO t_absorb_reuse SELECT number,         number % 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_absorb_reuse SELECT number + 50000, number % 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_absorb_reuse_dim SELECT number FROM numbers(150);
OPTIMIZE TABLE t_absorb_reuse FINAL;

SELECT 'statistics materialized', max(level) >= 1
FROM system.parts WHERE database = currentDatabase() AND table = 't_absorb_reuse' AND active;

-- `NOT (b > c AND a > c)`: a conjunction of two unanalysable atoms, negated.
INSERT INTO t_absorb_reuse_estimates
SELECT 'negated_pair', toUInt64OrNull(extract(explain, 't_absorb_reuse\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_absorb_reuse INNER JOIN t_absorb_reuse_dim
      ON t_absorb_reuse_dim.id = t_absorb_reuse.a
      WHERE NOT (t_absorb_reuse.b > t_absorb_reuse.c AND t_absorb_reuse.a > t_absorb_reuse.c)
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_absorb_reuse_estimates
SELECT 'range', toUInt64OrNull(extract(explain, 't_absorb_reuse\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_absorb_reuse INNER JOIN t_absorb_reuse_dim
      ON t_absorb_reuse_dim.id = t_absorb_reuse.a
      WHERE t_absorb_reuse.b < 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

-- The negated clause is reused in a wider conjunction, which is where the factor could be applied
-- a second time.
INSERT INTO t_absorb_reuse_estimates
SELECT 'negated_pair_and_range', toUInt64OrNull(extract(explain, 't_absorb_reuse\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_absorb_reuse INNER JOIN t_absorb_reuse_dim
      ON t_absorb_reuse_dim.id = t_absorb_reuse.a
      WHERE NOT (t_absorb_reuse.b > t_absorb_reuse.c AND t_absorb_reuse.a > t_absorb_reuse.c)
        AND t_absorb_reuse.b < 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

SELECT 'every estimate was extracted', count() = 3 AND countIf(estimated_rows > 0) = 3
FROM t_absorb_reuse_estimates;

-- The estimator has no notion of correlation between the negated pair and the range, so the
-- conjunction is their product. Carrying the folded factor over as well multiplies it in again,
-- which here is another `default_unknown_cond_factor` squared - an order of magnitude.
SELECT 'a negated absorbing clause is not counted twice',
       abs(toInt64((SELECT estimated_rows FROM t_absorb_reuse_estimates WHERE name = 'negated_pair_and_range'))
           - toInt64((SELECT estimated_rows FROM t_absorb_reuse_estimates WHERE name = 'negated_pair')
                   * (SELECT estimated_rows FROM t_absorb_reuse_estimates WHERE name = 'range') / 100000)) <= 2;

DROP TABLE t_absorb_reuse_estimates;
DROP TABLE t_absorb_reuse_dim;
DROP TABLE t_absorb_reuse;
