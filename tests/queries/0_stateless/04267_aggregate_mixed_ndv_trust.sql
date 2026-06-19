-- One grouping key has NDV stats, the other does not. The aggregate row count
-- then falls back to `input_stats.estimated_rows` instead of a product of NDVs.
-- That fabricated estimate must not cross the trust boundary: any join that
-- consumes the aggregation result is itself untrusted, and the outer optimizer
-- must read `ResultRows: unknown` for the wrapping join.

SET allow_experimental_statistics = 1;

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_algorithm = 'greedy';

DROP TABLE IF EXISTS t_facts_04267;
DROP TABLE IF EXISTS t_dim_inner_04267;
DROP TABLE IF EXISTS t_dim_outer_04267;

-- `k_with_stats` has `uniq` stats, `k_no_stats` does not. `auto_statistics_types`
-- is cleared at the table level so the runner cannot auto-attach NDV stats to
-- `k_no_stats` and mask the mixed-NDV case under test.
CREATE TABLE t_facts_04267
(
    k_with_stats UInt64 STATISTICS(uniq),
    k_no_stats UInt64,
    v UInt64
)
ENGINE = MergeTree ORDER BY k_with_stats SETTINGS auto_statistics_types = '';

CREATE TABLE t_dim_inner_04267 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_dim_outer_04267 (k UInt64, v UInt64) ORDER BY k SETTINGS auto_statistics_types = 'uniq';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_facts_04267     SELECT number % 1000, number % 50, number FROM numbers(10000);
INSERT INTO t_dim_inner_04267 SELECT number, number FROM numbers(100);
INSERT INTO t_dim_outer_04267 SELECT number, number FROM numbers(100);

OPTIMIZE TABLE t_facts_04267 FINAL;

-- Aggregation with one missing grouping-key NDV feeds an inner join. The inner
-- join is untrusted, so the outer join must show `ResultRows: unknown`.
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_dim_outer_04267 o
    JOIN (
        SELECT agg.k AS k
        FROM (
            SELECT k_with_stats AS k
            FROM t_facts_04267
            GROUP BY k_with_stats, k_no_stats
        ) agg
        JOIN t_dim_inner_04267 di ON agg.k = di.k
    ) sub
      ON o.k = sub.k
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_facts_04267;
DROP TABLE t_dim_inner_04267;
DROP TABLE t_dim_outer_04267;
