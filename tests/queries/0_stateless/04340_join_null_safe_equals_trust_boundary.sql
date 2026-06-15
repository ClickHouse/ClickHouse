-- A nested inner join whose ON clause is a null-safe equality
-- (`a.k IS NOT DISTINCT FROM b.k`, i.e. `NullSafeEquals`). The DP estimates the
-- selectivity of null-safe equality like a plain equality, so even with fully
-- stats-backed keys the row estimate is computed. However, the NDV tightening in
-- the `DPJoinEntry` constructor and the equivalence classes only handle plain
-- `Equals`, so the propagated key NDVs are left unreduced. The estimate must
-- therefore not cross the trust boundary: the outer optimizer must read
-- `ResultRows: unknown` for the inner join, just like for residual predicates.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE IF EXISTS t_outer_04340;
DROP TABLE IF EXISTS t_inner_a_04340;
DROP TABLE IF EXISTS t_inner_b_04340;

CREATE TABLE t_outer_04340  (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_a_04340 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_b_04340 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_outer_04340  SELECT number, number FROM numbers(100);
INSERT INTO t_inner_a_04340 SELECT number, number FROM numbers(1000);
INSERT INTO t_inner_b_04340 SELECT number, number FROM numbers(1000);

-- The inner join's keys are fully stats-backed, but the null-safe equality does
-- not tighten the propagated NDVs, so its estimate is demoted to untrusted. The
-- inner join's `ResultRows` must therefore read `unknown` upstream.
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_outer_04340 o
    JOIN (SELECT a.k AS k FROM t_inner_a_04340 a JOIN t_inner_b_04340 b ON a.k IS NOT DISTINCT FROM b.k) sub
      ON o.k = sub.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_outer_04340;
DROP TABLE t_inner_a_04340;
DROP TABLE t_inner_b_04340;
