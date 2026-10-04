-- `indexHint` carries a copy of conditions that the expression already contains, kept so that index
-- analysis can still see them after they were moved to `PREWHERE`. It evaluates to 1 for every row,
-- so it filters nothing. The selectivity estimator used to descend into it like into an `AND`, which
-- counted those conditions a second time: a conjunction came out several times more selective than
-- the product of its parts, and how many times depended on how many hints the plan happened to carry.
--
-- The assertion compares an estimate against the product of the estimates of its two conjuncts rather
-- than against a fixed number, so it holds whatever the default factors are.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_index_hint_est;
DROP TABLE IF EXISTS t_index_hint_dim;
DROP TABLE IF EXISTS t_index_hint_estimates;

CREATE TABLE t_index_hint_est (a UInt64, b UInt64, c UInt64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_index_hint_dim (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic, uniq_v2';

CREATE TABLE t_index_hint_estimates (name String, estimated_rows Nullable(UInt64)) ENGINE = Memory;

-- Statistics are materialized by a merge, so two parts and an OPTIMIZE are required.
INSERT INTO t_index_hint_est SELECT number,         number % 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_index_hint_est SELECT number + 50000, number % 50000, number % 50000 FROM numbers(50000);
INSERT INTO t_index_hint_dim SELECT number FROM numbers(150);
OPTIMIZE TABLE t_index_hint_est FINAL;

SELECT 'statistics materialized', max(level) >= 1
FROM system.parts WHERE database = currentDatabase() AND table = 't_index_hint_est' AND active;

-- `b < 1000` is an ordinary range atom; `b > c` compares two columns and cannot be analysed. Neither
-- query alone leaves an `indexHint` behind, but their conjunction does.
INSERT INTO t_index_hint_estimates
SELECT 'range', toUInt64OrNull(extract(explain, 't_index_hint_est\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_index_hint_est INNER JOIN t_index_hint_dim
      ON t_index_hint_dim.id = t_index_hint_est.a
      WHERE t_index_hint_est.b < 1000
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_index_hint_estimates
SELECT 'unknown', toUInt64OrNull(extract(explain, 't_index_hint_est\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_index_hint_est INNER JOIN t_index_hint_dim
      ON t_index_hint_dim.id = t_index_hint_est.a
      WHERE t_index_hint_est.b > t_index_hint_est.c
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

INSERT INTO t_index_hint_estimates
SELECT 'both', toUInt64OrNull(extract(explain, 't_index_hint_est\\[(\\d+)\\]'))
FROM (EXPLAIN PLAN keep_logical_steps = 1
      SELECT count() FROM t_index_hint_est INNER JOIN t_index_hint_dim
      ON t_index_hint_dim.id = t_index_hint_est.a
      WHERE t_index_hint_est.b < 1000 AND t_index_hint_est.b > t_index_hint_est.c
      SETTINGS use_statistics = 1, enable_cascades_optimizer = 0, enable_parallel_replicas = 0,
               query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0,
               use_hash_table_stats_for_join_reordering = 0)
WHERE explain LIKE '%Join:%' LIMIT 1;

SELECT 'every estimate was extracted', count() = 3 AND countIf(estimated_rows > 0) = 3
FROM t_index_hint_estimates;

-- The two conjuncts are on unrelated things, so the estimator treats them as independent: the
-- conjunction has to come out as their product. Counting the hint's copy of `b > c` a second time
-- made it a further `default_unknown_cond_factor` smaller.
SELECT 'conjunction is the product of its parts',
       abs((SELECT estimated_rows FROM t_index_hint_estimates WHERE name = 'both')
           - (SELECT estimated_rows FROM t_index_hint_estimates WHERE name = 'range')
             * (SELECT estimated_rows FROM t_index_hint_estimates WHERE name = 'unknown') / 100000) <= 2;

DROP TABLE t_index_hint_estimates;
DROP TABLE t_index_hint_dim;
DROP TABLE t_index_hint_est;
