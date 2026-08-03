SELECT count()
FROM (SELECT number AS k FROM numbers(200000) ORDER BY k ASC) AS l
INNER JOIN (SELECT number % NULL AS k FROM numbers(100) ORDER BY k) AS r ON l.k = r.k
SETTINGS join_algorithm = 'parallel_full_sorting_merge', max_threads = 4, max_block_size = 16;
