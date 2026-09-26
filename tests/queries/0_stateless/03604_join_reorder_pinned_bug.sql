SET query_plan_optimize_join_order_limit = 4;

SELECT 1
      FROM (SELECT 1 c0 LIMIT 1) AS t1
 LEFT JOIN (SELECT 1 c0 LIMIT 1) t3 ON t1.c0 = t3.c0
INNER JOIN (SELECT 1 c0 LIMIT 1) t5 ON t3.c0 = t5.c0
INNER JOIN (SELECT 1 c0 LIMIT 1) t7 ON t5.c0 = t7.c0
;

