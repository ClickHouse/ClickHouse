-- A nested join exposes its result-row estimate to the outer optimizer only
-- when the estimate is trustworthy. The DP cardinality formula models
-- `ALL`-join semantics (`selectivity * lhs * rhs`); for `ANY` / `SEMI` / `ANTI`
-- joins the real output is bounded differently, so even with column NDVs
-- available the estimate is not representative and must stay untrusted upstream
-- (outer `ResultRows: unknown`). The `ALL` variant of the same shape does
-- propagate its trusted estimate.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET any_join_distinct_right_table_keys = 1;

DROP TABLE IF EXISTS t_outer_04279;
DROP TABLE IF EXISTS t_inner_a_04279;
DROP TABLE IF EXISTS t_inner_b_04279;

-- With auto_statistics_types = 'uniq' the column NDVs are built on INSERT,
-- so every NDV along the DP path is known and the inner-join estimate would
-- be trusted if join strictness were not taken into account.
CREATE TABLE t_outer_04279   (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_a_04279 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_b_04279 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_outer_04279   SELECT number, number FROM numbers(100);
INSERT INTO t_inner_a_04279 SELECT number, number FROM numbers(1000);
INSERT INTO t_inner_b_04279 SELECT number, number FROM numbers(1000);

SELECT '-- ALL inner join: trusted estimate propagates upstream';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_outer_04279 o
    JOIN (SELECT a.k AS k FROM t_inner_a_04279 a JOIN t_inner_b_04279 b ON a.k = b.k) sub
      ON o.k = sub.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

SELECT '-- ANY inner join: estimate is non-representative and hidden from upstream';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_outer_04279 o
    JOIN (SELECT a.k AS k FROM t_inner_a_04279 a ANY JOIN t_inner_b_04279 b ON a.k = b.k) sub
      ON o.k = sub.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_outer_04279;
DROP TABLE t_inner_a_04279;
DROP TABLE t_inner_b_04279;
