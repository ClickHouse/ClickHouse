-- A join whose ON clause mixes an equality with a residual inequality
-- (`a.k = b.k AND a.v < b.v`). The DP estimates the selectivity of the
-- equality only; the inequality is handled as a residual/post-join filter and
-- its selectivity is not accounted for. Even though both join keys have NDV
-- stats, the estimate silently ignores a semantics-affecting predicate, so it
-- must not cross the trust boundary: the outer optimizer must read
-- `ResultRows: unknown` for the inner join.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE IF EXISTS t_outer_04268;
DROP TABLE IF EXISTS t_inner_a_04268;
DROP TABLE IF EXISTS t_inner_b_04268;

CREATE TABLE t_outer_04268  (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_a_04268 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_b_04268 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_outer_04268  SELECT number, number FROM numbers(100);
INSERT INTO t_inner_a_04268 SELECT number, number FROM numbers(1000);
INSERT INTO t_inner_b_04268 SELECT number, number FROM numbers(1000);

-- The inner join has a trusted equality on fully-stats-backed keys, but the
-- additional `a.v < b.v` predicate is not estimated. The inner join's
-- `ResultRows` must therefore read `unknown` upstream.
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_outer_04268 o
    JOIN (SELECT a.k AS k FROM t_inner_a_04268 a JOIN t_inner_b_04268 b ON a.k = b.k AND a.v < b.v) sub
      ON o.k = sub.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_outer_04268;
DROP TABLE t_inner_a_04268;
DROP TABLE t_inner_b_04268;
