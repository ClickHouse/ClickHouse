SET min_joined_block_size_bytes = 0;
SET max_block_size = 6;
SET query_plan_join_swap_table=false;
SET join_algorithm='hash';

SELECT blockSize() bs FROM (SELECT 1 s) js1 ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3]) s) js2 USING (s) GROUP BY bs ORDER BY bs;
