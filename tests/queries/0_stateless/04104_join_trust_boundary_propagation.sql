-- A nested JoinStepLogical exposes its result-row estimate to the outer
-- optimizer only when the estimate is trusted (column NDVs available).
-- Untrusted estimates stay as `ResultRows: unknown` upstream.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE IF EXISTS t_outer_04104;
DROP TABLE IF EXISTS t_inner_a_04104;
DROP TABLE IF EXISTS t_inner_b_04104;

-- With materialize_statistics_on_insert=1 the `uniq` stats are built on INSERT,
-- so the optimizer has NDVs and the inner-join result is trusted.
CREATE TABLE t_outer_04104   (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_a_04104 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_b_04104 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_outer_04104   SELECT number, number FROM numbers(100);
INSERT INTO t_inner_a_04104 SELECT number, number FROM numbers(1000);
INSERT INTO t_inner_b_04104 SELECT number, number FROM numbers(1000);

SELECT '-- trusted: stats materialized, inner join cardinality propagates';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_outer_04104 o
    JOIN (SELECT a.k AS k FROM t_inner_a_04104 a JOIN t_inner_b_04104 b ON a.k = b.k) sub
      ON o.k = sub.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_outer_04104;
DROP TABLE t_inner_a_04104;
DROP TABLE t_inner_b_04104;

-- Same shape without stats: NDV-unknown fallback fires, inner join is untrusted,
-- outer optimizer must see `ResultRows: unknown`.
CREATE TABLE t_outer_04104   (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_inner_a_04104 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = '';
CREATE TABLE t_inner_b_04104 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = '';

INSERT INTO t_outer_04104   SELECT number, number FROM numbers(100);
INSERT INTO t_inner_a_04104 SELECT number, number FROM numbers(1000);
INSERT INTO t_inner_b_04104 SELECT number, number FROM numbers(1000);

SELECT '-- untrusted: no stats, inner join cardinality is hidden from upstream';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_outer_04104 o
    JOIN (SELECT a.k AS k FROM t_inner_a_04104 a JOIN t_inner_b_04104 b ON a.k = b.k) sub
      ON o.k = sub.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_outer_04104;
DROP TABLE t_inner_a_04104;
DROP TABLE t_inner_b_04104;
