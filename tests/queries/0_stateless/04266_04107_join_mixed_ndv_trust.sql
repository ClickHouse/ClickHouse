-- One join key has NDV stats, the other does not. The selectivity estimate
-- then depends on a fabricated denominator and must not cross the trust
-- boundary, so the nested join's `ResultRows` must read `unknown` upstream.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE IF EXISTS t_outer_04266;
DROP TABLE IF EXISTS t_inner_with_stats_04266;
DROP TABLE IF EXISTS t_inner_no_stats_04266;

CREATE TABLE t_outer_04266            (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_with_stats_04266 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_inner_no_stats_04266   (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = '';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_outer_04266            SELECT number, number FROM numbers(100);
INSERT INTO t_inner_with_stats_04266 SELECT number, number FROM numbers(1000);
INSERT INTO t_inner_no_stats_04266   SELECT number, number FROM numbers(1000);

-- Mixed NDV in the nested join: a.k has `uniq` stats, b.k does not. The
-- partially-fabricated cardinality must stay inside the DP and the outer
-- optimizer must see `ResultRows: unknown` for the inner join.
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_outer_04266 o
    JOIN (SELECT a.k AS k FROM t_inner_with_stats_04266 a JOIN t_inner_no_stats_04266 b ON a.k = b.k) sub
      ON o.k = sub.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_outer_04266;
DROP TABLE t_inner_with_stats_04266;
DROP TABLE t_inner_no_stats_04266;
