SET group_by_each_block_no_merge = 1;
SET max_block_size = 1000;
SELECT * FROM (SELECT number DIV 113 AS k, count() AS c, sum(number) FROM system.numbers GROUP BY ALL LIMIT 100) ORDER BY k;
