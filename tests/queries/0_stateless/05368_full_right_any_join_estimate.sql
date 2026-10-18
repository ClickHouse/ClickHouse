-- A `FULL` join with distinct right keys (`any_join_distinct_right_table_keys`) emits every left row once and
-- also the right rows no left row matches, so it emits at least as many rows as the larger side. The estimate
-- and the bound must not stop at the right side: 1000 left rows meet 100 right rows here and 1095 rows come out.
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 0;
SET any_join_distinct_right_table_keys = 1;
-- The estimate uses the distinct counts of the keys.
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_fra_left;
DROP TABLE IF EXISTS t_fra_right;
CREATE TABLE t_fra_left (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_fra_right (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_fra_left SELECT number % 5 FROM numbers(1000);
INSERT INTO t_fra_right SELECT number FROM numbers(100);

SELECT 'true rows', count() FROM t_fra_left AS l ANY FULL JOIN t_fra_right AS r ON l.k = r.k;

SELECT replaceRegexpAll(regexpExtract(explain, '\\(rows: .*\\)$'), ', cost: [0-9.e+]+', '')
FROM (
    EXPLAIN estimates = 1, bounds = 1
    SELECT * FROM t_fra_left AS l ANY FULL JOIN t_fra_right AS r ON l.k = r.k
)
WHERE explain LIKE '%Join (%';

DROP TABLE t_fra_left;
DROP TABLE t_fra_right;
