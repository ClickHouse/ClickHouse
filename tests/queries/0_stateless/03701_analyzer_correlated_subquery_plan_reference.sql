SET enable_analyzer = 1;
SET query_plan_join_swap_table = false;
SET enable_parallel_replicas = 0;
SET correlated_subqueries_default_join_kind = 'left';

CREATE TABLE t(x Int, y Int) ORDER BY ()
AS SELECT number as x, number % 2 as y FROM numbers(100);

EXPLAIN actions = 1
SELECT
    count()
FROM
    t n
WHERE
  EXISTS (
    SELECT
        *
    FROM
        numbers(10)
    WHERE
        number != n.x
  )
  AND n.y = 1;
