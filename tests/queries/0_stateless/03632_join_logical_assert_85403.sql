SELECT 1 FROM (SELECT 1 x, 1 y) a JOIN (SELECT 1 y) b USING (y) WHERE round(*) = b.y;

SET query_plan_use_new_logical_join_step = 0;
SELECT 1 FROM (SELECT 1 x, 1 y) a JOIN (SELECT 1 y) b USING (y) WHERE round(*) = b.y;
