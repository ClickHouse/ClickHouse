SET enable_analyzer = 1;
SET query_plan_convert_any_join_to_semi_or_anti_join = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;

EXPLAIN actions = 1, keep_logical_steps = 1
SELECT 1 FROM 
    (SELECT 1) tx 
  ANY LEFT JOIN 
    (SELECT 1 c0) tz 
  ON TRUE 
WHERE tz.c0 = materialize(1);

SELECT 1 FROM 
    (SELECT 1) tx 
  ANY LEFT JOIN 
    (SELECT 1 c0) tz 
  ON TRUE 
WHERE tz.c0 = materialize(1);
