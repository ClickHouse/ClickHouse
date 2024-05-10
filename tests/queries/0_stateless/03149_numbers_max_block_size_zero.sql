SELECT count(*) FROM numbers(10) AS a, numbers(11) AS b, numbers(12) AS c SETTINGS max_block_size = 0;
SELECT count(*) FROM numbers(10) AS a, numbers(11) AS b, numbers(12) AS c SETTINGS max_block_size = 1;
