-- `EXPLAIN PLAN actions = 1` shows the boundary expressions of a range step, so plans that differ only in
-- their boundaries or in whether they read all data are distinguishable.
SET explain_query_plan_default = 'legacy';

SELECT trimLeft(explain) FROM (EXPLAIN PLAN actions = 1 SELECT number FROM numbers(5) ORDER BY number LIMIT 2 AFTER number >= 1 UNTIL number >= 4 SETTINGS exact_rows_before_limit = 0)
WHERE explain LIKE '%LimitRange%' OR explain LIKE '%Limit 2%' OR explain LIKE '%column:%' OR explain LIKE '%FUNCTION%' OR explain LIKE '%Reads all data%';

SELECT trimLeft(explain) FROM (EXPLAIN PLAN actions = 1 SELECT number FROM numbers(5) ORDER BY number LIMIT 2 AFTER number = 3 ALL SETTINGS exact_rows_before_limit = 1)
WHERE explain LIKE '%LimitRange%' OR explain LIKE '%Limit 2%' OR explain LIKE '%column:%' OR explain LIKE '%FUNCTION%' OR explain LIKE '%Reads all data%';

SELECT explain LIKE '%"After Column"%' AND explain LIKE '%"Until Column"%' AND explain LIKE '%"Expression"%' AND explain LIKE '%"After All": false%' AND explain LIKE '%"Reads All Data": false%'
FROM (EXPLAIN PLAN json = 1, actions = 1 SELECT number FROM numbers(5) ORDER BY number LIMIT 2 AFTER number >= 1 UNTIL number >= 4 SETTINGS exact_rows_before_limit = 0);
