-- A nested join whose subquery has a post-join filter referencing columns from
-- both sides (e.g. `WHERE a.v < b.v`) cannot be pushed into either table scan,
-- so it stays as a `FilterStep` above the inner `JoinStepLogical`. That filter is
-- not estimated against the join output, so the inner join cardinality is no
-- longer representative of the subquery's real output and must stay untrusted
-- upstream (outer `ResultRows: unknown`), even when all column NDVs are available.
-- Without the post-join filter the same shape propagates its trusted estimate.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE IF EXISTS t_outer_04309;
DROP TABLE IF EXISTS t_inner_a_04309;
DROP TABLE IF EXISTS t_inner_b_04309;

-- With auto_statistics_types = 'uniq' the column NDVs are built on INSERT,
-- so every NDV along the DP path is known and the inner-join estimate is trusted.
CREATE TABLE t_outer_04309   (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_a_04309 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_b_04309 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_outer_04309   SELECT number, number FROM numbers(100);
INSERT INTO t_inner_a_04309 SELECT number, number FROM numbers(1000);
INSERT INTO t_inner_b_04309 SELECT number, number FROM numbers(1000);

SELECT '-- no post-join filter: trusted inner-join estimate propagates upstream';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_outer_04309 o
    JOIN (SELECT a.k AS k FROM t_inner_a_04309 a JOIN t_inner_b_04309 b ON a.k = b.k) sub
      ON o.k = sub.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

SELECT '-- post-join filter over both sides: estimate is hidden from upstream';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_outer_04309 o
    JOIN (SELECT a.k AS k FROM t_inner_a_04309 a JOIN t_inner_b_04309 b ON a.k = b.k WHERE a.v < b.v) sub
      ON o.k = sub.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_outer_04309;
DROP TABLE t_inner_a_04309;
DROP TABLE t_inner_b_04309;
