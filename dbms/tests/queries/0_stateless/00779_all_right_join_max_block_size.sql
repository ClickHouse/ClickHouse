SET max_block_size = 6;
SELECT blockSize() bs FROM (SELECT 1 s) ALL RIGHT JOIN (SELECT arrayJoin([2, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3]) s) USING (s) GROUP BY bs ORDER BY bs;
