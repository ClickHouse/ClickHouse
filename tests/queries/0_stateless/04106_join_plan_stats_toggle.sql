-- Star-join shape (fact, dimension, filter-dimension) on the same tables with
-- `uniq` column statistics materialized, toggled by `use_statistics`.
--
-- With `use_statistics = 0` the NDVs are hidden from the optimizer: the DP
-- cannot pre-join the two small dimensions and falls back to a left-deep plan
-- (`f ⋈ s ⋈ n`) where supp (100 rows) is the build against fact.
--
-- With `use_statistics = 1` the trusted NDVs let the DP pre-join the two small
-- dimensions, producing a right-deep plan (`f ⋈ (s ⋈ n)`) where the tiny
-- pre-joined result (10 rows) is on the build side. `ResultRows` propagates
-- numeric estimates end-to-end.
--
-- Settings pinned at session and inline level because CI injects random values
-- for `query_plan_join_swap_table`, `query_plan_optimize_join_order_limit`,
-- `query_plan_optimize_join_order_algorithm`, `enable_join_runtime_filters`,
-- and `query_plan_read_in_order_through_join`.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_algorithm = 'greedy';
SET query_plan_join_swap_table = 'auto';
SET query_plan_read_in_order_through_join = 0;
SET enable_join_runtime_filters = 1;

DROP TABLE IF EXISTS t_fact_04106;
DROP TABLE IF EXISTS t_supp_04106;
DROP TABLE IF EXISTS t_nation_04106;

CREATE TABLE t_fact_04106   (s_id UInt64, v UInt64)   ORDER BY s_id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_supp_04106   (id UInt64, n_id UInt64)  ORDER BY id   SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE t_nation_04106 (id UInt64, name String)  ORDER BY id   SETTINGS auto_statistics_types = 'uniq';

SET materialize_statistics_on_insert = 1;
INSERT INTO t_fact_04106   SELECT number % 100, number     FROM numbers(10000);
INSERT INTO t_supp_04106   SELECT number, number % 10      FROM numbers(100);
INSERT INTO t_nation_04106 SELECT number, toString(number) FROM numbers(10);

-- Inline SETTINGS reset unlisted values to CLI defaults, so repeat the
-- plan-affecting ones here.
SELECT '-- use_statistics = 0: left-deep plan, outer ResultRows untrusted';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_fact_04106 f
    JOIN t_supp_04106 s   ON f.s_id = s.id
    JOIN t_nation_04106 n ON s.n_id = n.id
    WHERE n.name = '3'
    SETTINGS use_statistics = 0, query_plan_join_swap_table = 'auto',
             query_plan_optimize_join_order_limit = 10,
             query_plan_optimize_join_order_algorithm = 'greedy',
             query_plan_read_in_order_through_join = 0,
             enable_join_runtime_filters = 1
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

SELECT '-- use_statistics = 1: dims pre-joined, trusted cardinalities propagate';
SELECT trimBoth(explain) FROM (
    EXPLAIN actions = 1, keep_logical_steps = 1
    SELECT count() FROM t_fact_04106 f
    JOIN t_supp_04106 s   ON f.s_id = s.id
    JOIN t_nation_04106 n ON s.n_id = n.id
    WHERE n.name = '3'
    SETTINGS use_statistics = 1, query_plan_join_swap_table = 'auto',
             query_plan_optimize_join_order_limit = 10,
             query_plan_optimize_join_order_algorithm = 'greedy',
             query_plan_read_in_order_through_join = 0,
             enable_join_runtime_filters = 1
) WHERE explain LIKE '%Join:%' OR explain LIKE '%ResultRows%';

DROP TABLE t_fact_04106;
DROP TABLE t_supp_04106;
DROP TABLE t_nation_04106;
